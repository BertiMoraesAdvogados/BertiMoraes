// Patch: WhatsApp Cloud API -> Maestro webhook genérico.
// Não contém segredos; exige variáveis de ambiente no Render.
// Variáveis: VERIFY_TOKEN (valor atual configurado no Meta), MAESTRO_URL, MAESTRO_SECRET.
// MESSAGE_DEBOUNCE_MS opcional; padrão 1800 ms.
'use strict';
const express = require('express');
const https = require('https');
const PORT = Number(process.env.PORT || 3000);
const VERIFY_TOKEN = process.env.VERIFY_TOKEN || '';
const MAESTRO_URL = process.env.MAESTRO_URL || '';
const MAESTRO_SECRET = process.env.MAESTRO_SECRET || '';
const parsedDebounce = Number(process.env.MESSAGE_DEBOUNCE_MS || 1800);
const DEBOUNCE_MS = Number.isFinite(parsedDebounce) && parsedDebounce >= 0 ? parsedDebounce : 1800;
const DEDUPE_TTL_MS = 24 * 60 * 60 * 1000;
const MAX_DEDUPE_ENTRIES = 50000;
function clone(value) { return value == null ? value : JSON.parse(JSON.stringify(value)); }
function createMessageBatcher({ onBatch, debounceMs = DEBOUNCE_MS, now = Date.now, logger = console } = {}) {
  if (typeof onBatch !== 'function') throw new TypeError('onBatch must be a function');
  const pending = new Map(), sentIds = new Map(), inFlight = new Map();
  let sequence = 0, anonymousId = 0;
  function prune(nowMs) {
    for (const [id, expiry] of sentIds) if (expiry <= nowMs) sentIds.delete(id);
    if (sentIds.size > MAX_DEDUPE_ENTRIES) { let remove = sentIds.size - MAX_DEDUPE_ENTRIES; for (const id of sentIds.keys()) { sentIds.delete(id); if (--remove <= 0) break; } }
  }
  function makeBatch(payload, entry, change, value, sender, phoneId) {
    const basePayload = clone(payload) || {}, baseEntry = clone(entry) || {}, baseChange = clone(change) || {}, baseValue = clone(value) || {};
    delete basePayload.entry; delete baseEntry.changes; delete baseChange.value; delete baseValue.messages; delete baseValue.statuses;
    return { key: `${phoneId}:${sender}`, basePayload, baseEntry, baseChange, baseValue, messages: new Map(), contacts: new Map(), timer: null };
  }
  function toPayload(batch) {
    const value = clone(batch.baseValue) || {};
    value.messages = [...batch.messages.values()].sort((a,b) => { const l=Number(a.message.timestamp), r=Number(b.message.timestamp), lv=Number.isFinite(l), rv=Number.isFinite(r); if(lv&&rv&&l!==r)return l-r; if(lv!==rv)return lv?-1:1; return a.sequence-b.sequence; }).map(x=>x.message);
    if(batch.contacts.size)value.contacts=[...batch.contacts.values()];
    const change={...clone(batch.baseChange),value}; return {...clone(batch.basePayload),entry:[{...clone(batch.baseEntry),changes:[change]}]};
  }
  function flush(key) {
    const batch=pending.get(key); if(!batch)return Promise.resolve(); if(batch.timer)clearTimeout(batch.timer); pending.delete(key);
    const payload=toPayload(batch), previous=inFlight.get(key)||Promise.resolve(); let task;
    task=previous.catch(()=>{}).then(()=>onBatch(payload,key)).catch(error=>logger.error?.('Falha ao encaminhar lote WhatsApp ao Maestro:',error?.message||error)).then(()=>{if(inFlight.get(key)===task)inFlight.delete(key);});
    inFlight.set(key,task); return task;
  }
  function enqueue(payload) {
    const entries=Array.isArray(payload?.entry)?payload.entry:[], nowMs=now(); prune(nowMs); let accepted=0,duplicates=0,ignored=0;
    for(const entry of entries) for(const change of Array.isArray(entry?.changes)?entry.changes:[]) {
      const value=change?.value, messages=Array.isArray(value?.messages)?value.messages:[]; if(!messages.length){ignored++;continue;}
      for(const message of messages) {
        if(!message?.from){ignored++;continue;}
        const phoneId=String(value?.metadata?.phone_number_id||entry?.id||'unknown-phone'), sender=String(message.from), id=message.id?`${phoneId}:${message.id}`:null;
        if(id&&sentIds.has(id)&&sentIds.get(id)>nowMs){duplicates++;continue;} if(id)sentIds.set(id,nowMs+DEDUPE_TTL_MS);
        const key=`${phoneId}:${sender}`; let batch=pending.get(key); if(!batch){batch=makeBatch(payload,entry,change,value,sender,phoneId);pending.set(key,batch);}
        batch.messages.set(id||`anonymous-${++anonymousId}`,{message:clone(message),sequence:sequence++});
        for(const contact of Array.isArray(value?.contacts)?value.contacts:[])if(contact?.wa_id&&String(contact.wa_id)===sender)batch.contacts.set(sender,clone(contact));
        if(batch.timer)clearTimeout(batch.timer); batch.timer=setTimeout(()=>{void flush(key);},debounceMs); accepted++;
      }
    }
    return {accepted,duplicates,ignored,pendingSenders:pending.size};
  }
  async function flushAll(){await Promise.all([...pending.keys()].map(flush));await Promise.all([...inFlight.values()]);}
  function close(){for(const batch of pending.values())if(batch.timer)clearTimeout(batch.timer);pending.clear();}
  return {enqueue,flushAll,close,get pendingCount(){return pending.size;}};
}
function postToMaestro(payload) {
  if(!MAESTRO_URL||!MAESTRO_SECRET)return Promise.reject(new Error('Set MAESTRO_URL and MAESTRO_SECRET in Render environment.'));
  let target; try{target=new URL(MAESTRO_URL);}catch(error){return Promise.reject(new Error(`MAESTRO_URL is invalid: ${error.message}`));}
  if(target.protocol!=='https:')return Promise.reject(new Error('MAESTRO_URL must use HTTPS.'));
  const body=JSON.stringify(payload);
  return new Promise((resolve,reject)=>{const req=https.request({hostname:target.hostname,path:`${target.pathname}${target.search}`,method:'POST',headers:{'Content-Type':'application/json','X-Webhook-Secret':MAESTRO_SECRET,'Content-Length':Buffer.byteLength(body)}},response=>{response.resume();response.on('end',()=>response.statusCode>=200&&response.statusCode<300?resolve():reject(new Error(`Maestro returned HTTP ${response.statusCode}`)));});req.setTimeout(10000,()=>req.destroy(new Error('Maestro request timed out')));req.on('error',reject);req.end(body);});
}
function createApp({batcher}={}) {
  const app=express(); app.use(express.json({limit:'1mb'})); const queue=batcher||createMessageBatcher({onBatch:postToMaestro,logger:console});
  app.get('/',(req,res)=>{const mode=req.query['hub.mode'],token=req.query['hub.verify_token'],challenge=req.query['hub.challenge']; if(mode==='subscribe'&&token===VERIFY_TOKEN&&challenge)return res.status(200).type('text/plain').send(challenge);return res.status(403).end();});
  app.get('/healthz',(_req,res)=>res.status(200).json({ok:true}));
  app.post('/',(req,res)=>{let stats;try{stats=queue.enqueue(req.body);}catch(error){console.error('Erro ao enfileirar evento WhatsApp:',error?.message||error);return res.status(200).json({received:true});}res.status(200).json({received:true});if(stats.accepted||stats.duplicates)console.log('Evento WhatsApp aceito:',JSON.stringify(stats));});
  return {app,batcher:queue};
}
if(require.main===module){if(!VERIFY_TOKEN)throw new Error('Set VERIFY_TOKEN to the value configured in Meta.');if(!MAESTRO_URL||!MAESTRO_SECRET)throw new Error('Set MAESTRO_URL and MAESTRO_SECRET in Render environment.');createApp().app.listen(PORT,()=>console.log(`WhatsApp webhook active; debounce=${DEBOUNCE_MS}ms`));}
module.exports={createApp,createMessageBatcher,postToMaestro};
