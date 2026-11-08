import { useState, useRef } from 'react';
import { createRoot } from 'react-dom/client';
import type { BootstrapRequest, SessionResult, Status, Capabilities, HistoryResult, HistoryEntry, RestoreRequest, RestoreResult } from './api';
import './style.css';

function App() {
 const [secret,setSecret]=useState('');
 const [token,setToken]=useState('');
 const [status,setStatus]=useState<Status|null>(null);
 const [caps,setCaps]=useState<Capabilities|null>(null);
 const [entries,setEntries]=useState<HistoryEntry[]>([]);
 const [cursor,setCursor]=useState('');
 const [selection,setSelection]=useState('');
 const [message,setMessage]=useState('');
 const [busy,setBusy]=useState(false);
 const resultRef=useRef<HTMLParagraphElement>(null);
 async function api<T>(path:string,body?:object,credential=token):Promise<T> {
  const response=await fetch(`/api/v1/${path}`,{method:body?'POST':'GET',headers:{...(credential?{Authorization:`Bearer ${credential}`} : {}),...(body?{'Content-Type':'application/json'}:{})},body:body?JSON.stringify(body):undefined,cache:'no-store',credentials:'omit'});
  const result:unknown=await response.json();
  if(!response.ok) {
   const detail=typeof result==='object' && result!==null && 'detail' in result?String(result.detail):'The service refused the request.';
   throw new Error(detail);
  }
  return result as T;
 }
 async function page(next:string,credential=token) {
  const history=await api<HistoryResult>(`history${next?`?cursor=${encodeURIComponent(next)}`:''}`,undefined,credential);
  setEntries(history.entries);setCursor(history.next_cursor);setSelection('');
 }
 async function authenticate(event:React.FormEvent) {
  event.preventDefault();setBusy(true);setMessage('');
  const request:BootstrapRequest={secret};setSecret('');
  try {
   const session=await api<SessionResult>('session',request);setToken(session.token);
   const [s,c]=await Promise.all([api<Status>('status',undefined,session.token),api<Capabilities>('capabilities',undefined,session.token)]);
   setStatus(s);setCaps(c);await page('',session.token);
  } catch(error) {setMessage(error instanceof Error?error.message:'Authentication failed.');}
  finally {setBusy(false);}
 }
 async function restore() {
  setBusy(true);setMessage('Restoring selected history…');
  try {
   const request:RestoreRequest={entry_id:selection};
   const result=await api<RestoreResult>('restore',request);
   setMessage(`${result.outcome}: ${result.detail}`);
  } catch(error) {
   setMessage(`${error instanceof Error?error.message:'Response lost.'} If the response was lost, outcome is unknown. Inspect the operator output before retrying.`);
  } finally {setBusy(false);resultRef.current?.focus();}
 }
 return <main>
  <p className="eyebrow">Coldkeep / action 2.02</p>
  <h1>Internal delivery proof</h1>
  <p className="notice">Disposable fixtures only. Product gates are incomplete. Native support, durable jobs and production authentication remain unqualified.</p>
  {!status?<form onSubmit={authenticate}>
   <h2>Open fixture session</h2>
   <label htmlFor="bootstrap">One-use bootstrap secret</label>
   <input id="bootstrap" type="password" autoComplete="off" value={secret} onChange={e=>setSecret(e.target.value)} required maxLength={64}/>
   <p>The operator supplies the private bootstrap. It expires after two minutes.</p>
   <button disabled={busy}>Authenticate</button>
  </form>:<>
   <section aria-labelledby="service"><h2 id="service">Service</h2>
    <dl><dt>Application / API</dt><dd>{status.application} / {status.api}</dd><dt>Backend</dt><dd>{status.backend}</dd><dt>Prepared source</dt><dd>{status.source_revision}{status.source_dirty?' (development build)':''}</dd><dt>Embedded assets</dt><dd>{status.asset_digest}</dd></dl>
    <p>History: {caps?.history?'available':'unavailable'} · Restore: {caps?.restore?'available':'unavailable'} · Search and durable jobs: unsupported</p>
   </section>
   <section aria-labelledby="history"><h2 id="history">Inherited fixture history</h2>
    <p>Exact selected historical names. Up to 32 entries per page.</p>
    <fieldset disabled={busy}><legend>Select one file to restore</legend>{entries.map(entry=><label className="entry" key={entry.id}><input type="radio" name="entry" value={entry.id} checked={selection===entry.id} onChange={()=>setSelection(entry.id)}/><span className="name">{entry.name}</span><span>{entry.size} bytes</span></label>)}</fieldset>
    {cursor && <button disabled={busy} onClick={()=>{void page(cursor).catch(e=>setMessage(String(e)))}}>Next page</button>}
    <p><strong>Destination:</strong> {status.destination}</p>
    <button disabled={!selection || busy || !caps?.restore} onClick={()=>{void restore()}}>Restore selected file</button>
    <p>Closing this page leaves the daemon in charge. A lost response has an unknown outcome; inspect the operator output before retrying.</p>
   </section>
  </>}
  <p role="status" tabIndex={-1} ref={resultRef} className="result">{message}</p>
 </main>;
}
const root=document.getElementById('root');if(root)createRoot(root).render(<App/>);
