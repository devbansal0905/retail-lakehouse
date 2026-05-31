"""Real-time dashboard backend (FastAPI + Server-Sent Events) with login.

Auth: a username/password login (users stored in Neo4j via auth.py) gates the
dashboard and all data endpoints with an HttpOnly session cookie. Each session
keeps its own NL-to-SQL conversation history.

Pages:   /login  /  (dashboard)  /chat  /quality
APIs:    /stream (SSE)  /api/kpis  /ask  /history  /logout
"""
from __future__ import annotations

import asyncio
import json
import os
import sys

from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, StreamingResponse

sys.path.insert(0, os.path.dirname(__file__))
import auth  # noqa: E402
import serving  # noqa: E402

app = FastAPI(title="Retail Lakehouse - Realtime")
COOKIE = "rl_session"


@app.on_event("startup")
def _startup():
    try:
        auth.seed_default_user()
    except Exception:  # noqa: BLE001 - graph may still be booting; store degrades to memory
        pass


def _token(request: Request) -> str | None:
    return request.cookies.get(COOKIE)


def _require(request: Request):
    return auth.get_session(_token(request))


def _payload() -> dict | None:
    """Read the live dashboard payload straight from the Delta tables."""
    try:
        return serving.build_payload()
    except Exception:  # noqa: BLE001 - tables may still be initialising
        return None


def _version() -> str | None:
    """Delta-version change token the SSE loop watches."""
    try:
        return serving.latest_version()
    except Exception:  # noqa: BLE001
        return None


# --------------------------------- auth --------------------------------------

@app.get("/login", response_class=HTMLResponse)
def login_page(err: str = ""):
    msg = f'<p class="err">{err}</p>' if err else ""
    return HTMLResponse(_LOGIN_HTML.replace("{{ERR}}", msg))


@app.post("/login")
def login(username: str = Form(...), password: str = Form(...)):
    if auth.verify(username, password):
        token = auth.new_session(username)
        resp = RedirectResponse("/", status_code=303)
        resp.set_cookie(COOKIE, token, httponly=True, samesite="lax")
        return resp
    return RedirectResponse("/login?err=Invalid+username+or+password", status_code=303)


@app.get("/logout")
def logout(request: Request):
    auth.end_session(_token(request))
    resp = RedirectResponse("/login", status_code=303)
    resp.delete_cookie(COOKIE)
    return resp


# ------------------------------- data API ------------------------------------

@app.get("/api/kpis")
def api_kpis(request: Request):
    if not _require(request):
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    return JSONResponse(_payload() or {"version": -1, "overview": {}, "country": [],
                                       "top_products": [], "customers": []})


@app.get("/stream")
async def stream(request: Request):
    if not _require(request):
        return JSONResponse({"error": "unauthorized"}, status_code=401)

    async def gen():
        last = None
        p = _payload()
        if p:
            yield f"data: {json.dumps(p)}\n\n"
            last = str(p.get("version"))
        while True:
            await asyncio.sleep(1)
            v = _version()
            if v is not None and v != last:
                p = _payload()
                if p:
                    yield f"data: {json.dumps(p)}\n\n"
                    last = v
            else:
                yield ": keep-alive\n\n"
    return StreamingResponse(gen(), media_type="text/event-stream")


@app.get("/ask")
def ask(request: Request, q: str):
    sess = _require(request)
    if not sess:
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    import nl_to_sql
    result = nl_to_sql.run_over_live(q)
    auth.add_history(_token(request), {"q": q, **result})
    return JSONResponse(result)


@app.get("/history")
def get_history(request: Request):
    if not _require(request):
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    return JSONResponse({"history": auth.history(_token(request))})


# --------------------------------- pages -------------------------------------

@app.get("/", response_class=HTMLResponse)
def index(request: Request):
    sess = _require(request)
    if not sess:
        return RedirectResponse("/login", status_code=303)
    return HTMLResponse(_HTML.replace("{{USER}}", sess["username"]))


@app.get("/chat", response_class=HTMLResponse)
def chat(request: Request):
    sess = _require(request)
    if not sess:
        return RedirectResponse("/login", status_code=303)
    return HTMLResponse(_CHAT_HTML.replace("{{USER}}", sess["username"]))


@app.get("/quality", response_class=HTMLResponse)
def quality(request: Request):
    sess = _require(request)
    if not sess:
        return RedirectResponse("/login", status_code=303)
    return HTMLResponse(_QUALITY_HTML.replace("{{USER}}", sess["username"]))


# --------------------------------- styles ------------------------------------
# Shared light/modern design system, inlined into each page.
_CSS = """
  :root{
    --bg:#f5f6fa;--surface:#ffffff;--surface2:#fbfcfe;--border:#e6e8ee;--line:#eef0f4;
    --text:#0f172a;--muted:#667085;--accent:#4f46e5;--accent-weak:#eef2ff;
    --green:#16a34a;--green-bg:#ecfdf3;--amber:#d97706;--red:#dc2626;--red-bg:#fef3f2;
    --radius:14px;--shadow:0 1px 3px rgba(16,24,40,.07),0 1px 2px rgba(16,24,40,.04)}
  *{box-sizing:border-box}
  body{margin:0;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;
    color:var(--text);background:var(--bg);min-height:100vh;font-size:14px}
  header{position:sticky;top:0;z-index:5;background:var(--surface);border-bottom:1px solid var(--border);
    padding:12px 28px;display:flex;align-items:center;gap:14px}
  .brand b{font-size:15px;font-weight:600}.brand span{font-size:12px;color:var(--muted);display:block}
  .spacer{flex:1}
  .badge{display:flex;align-items:center;gap:7px;padding:5px 11px;border-radius:999px;font-size:12px;font-weight:600}
  .badge.ok{background:var(--green-bg);color:var(--green)}.badge.bad{background:var(--red-bg);color:var(--red)}
  .dot{width:7px;height:7px;border-radius:50%;background:currentColor}
  .chip{font-size:13px;color:var(--muted);background:var(--surface);border:1px solid var(--border);
    padding:6px 12px;border-radius:9px;text-decoration:none;transition:background .15s}
  a.chip:hover{background:var(--line);color:var(--text)}
  .wrap{padding:24px 28px 48px;max-width:1200px;margin:0 auto}
  .cards{display:grid;gap:14px;margin-bottom:18px}
  .card{position:relative;overflow:hidden;background:var(--surface);border:1px solid var(--border);
    border-radius:var(--radius);padding:16px 18px;box-shadow:var(--shadow)}
  .card .label{font-size:11px;color:var(--muted);text-transform:uppercase;letter-spacing:.5px;font-weight:600}
  .card .val{font-size:26px;font-weight:600;margin-top:8px;font-variant-numeric:tabular-nums}
  .card .sub{font-size:12px;color:var(--muted);margin-top:4px}
  .card.acc::before{content:"";position:absolute;left:0;top:0;bottom:0;width:3px;background:var(--accent)}
  .panel{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);padding:18px;box-shadow:var(--shadow)}
  .panel h3{margin:0 0 14px;font-size:14px;font-weight:600}.panel h3 small{color:var(--muted);font-weight:400;margin-left:6px}
  table{width:100%;border-collapse:collapse;font-size:13px}
  th{text-align:left;color:var(--muted);font-size:11px;text-transform:uppercase;letter-spacing:.4px;font-weight:600;padding:8px 10px;border-bottom:1px solid var(--border)}
  td{padding:9px 10px;border-bottom:1px solid var(--line);font-variant-numeric:tabular-nums}
  tr:last-child td{border-bottom:0}.num{text-align:right}
  input{background:var(--surface);border:1px solid var(--border);color:var(--text);border-radius:10px;padding:11px 13px;font-size:14px;outline:none}
  input:focus{border-color:var(--accent);box-shadow:0 0 0 3px var(--accent-weak)}
  button{background:var(--accent);color:#fff;border:0;border-radius:10px;padding:0 18px;font-weight:600;cursor:pointer}
  button:hover{filter:brightness(1.05)}
"""
_CHART_JS = """
const PALETTE=["#4f46e5","#16a34a","#0ea5e9","#f59e0b","#db2777","#14b8a6","#ef4444","#8b5cf6","#22c55e","#eab308"];
const GRID="#eef0f4",TICK="#667085";
function bar(ctx,horizontal){return new Chart(ctx,{type:"bar",
  data:{labels:[],datasets:[{data:[],backgroundColor:PALETTE,borderRadius:6,maxBarThickness:34}]},
  options:{indexAxis:horizontal?"y":"x",plugins:{legend:{display:false}},
    scales:{x:{grid:{color:GRID},ticks:{color:TICK}},y:{grid:{color:GRID},ticks:{color:TICK}}}}});}
"""


def _page(body: str, extra_css: str = "") -> str:
    # Any <script> accidentally passed via extra_css belongs in the body, not
    # inside <style>. Split it out so page scripts actually execute.
    css, _sep, script = extra_css.partition("<script>")
    script = ("<script>" + script) if script else ""
    return ("<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\"/>"
            "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\"/>"
            "<title>Retail Lakehouse</title>"
            "<script src=\"https://cdn.jsdelivr.net/npm/chart.js@4\"></script>"
            f"<style>{_CSS}{css}</style></head><body>{body}{script}</body></html>")


_LOGIN_HTML = ("<!doctype html><html><head><meta charset=\"utf-8\"/>"
    "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\"/><title>Sign in</title>"
    f"<style>{_CSS}"
    ".loginwrap{min-height:100vh;display:grid;place-items:center}"
    ".loginwrap .card{width:340px;padding:30px}"
    ".loginwrap h1{font-size:18px;margin:0 0 4px}.loginwrap p.sub{color:var(--muted);font-size:13px;margin:0 0 18px}"
    ".loginwrap label{display:block;font-size:12px;color:var(--muted);margin:12px 0 5px;font-weight:600}"
    ".loginwrap input{width:100%}.loginwrap button{width:100%;margin-top:18px;padding:12px}"
    ".err{color:var(--red);font-size:13px;margin-top:10px}"
    "</style></head><body><div class=\"loginwrap\"><form class=\"card\" method=\"post\" action=\"/login\">"
    "<h1>Retail Lakehouse</h1><p class=\"sub\">Sign in to the live dashboard</p>"
    "<label>Username</label><input name=\"username\" autofocus autocomplete=\"username\"/>"
    "<label>Password</label><input name=\"password\" type=\"password\" autocomplete=\"current-password\"/>"
    "<button type=\"submit\">Sign in</button>{{ERR}}</form></div></body></html>")


_NAV = ('<div class="brand"><b>Retail Lakehouse</b><span>__SUB__</span></div><div class="spacer"></div>'
        '<div class="badge ok"><span class="dot"></span>LIVE</div>'
        '<div class="chip" id="ver">v-</div><div class="chip" id="upd">waiting...</div>'
        '__LINKS__<div class="chip">{{USER}}</div><a class="chip" href="/logout">Sign out</a>')


_HTML = _page(
    "<header>" + _NAV.replace("__SUB__", "Real-time streaming analytics").replace(
        "__LINKS__", '<a class="chip" href="/chat">Ask the data</a><a class="chip" href="/quality">Data quality</a>')
    + "</header>"
    '<div class="wrap">'
    '<div class="cards" style="grid-template-columns:repeat(5,1fr)">'
    '<div class="card acc"><div class="label">Total revenue</div><div class="val" id="rev">-</div><div class="sub">net of cancellations</div></div>'
    '<div class="card acc"><div class="label">Orders</div><div class="val" id="ord">-</div><div class="sub">distinct invoices</div></div>'
    '<div class="card acc"><div class="label">Avg order value</div><div class="val" id="aov">-</div><div class="sub">revenue / orders</div></div>'
    '<div class="card acc"><div class="label">Customers</div><div class="val" id="cust">-</div><div class="sub">unique buyers</div></div>'
    '<div class="card acc"><div class="label">Revenue / customer</div><div class="val" id="rpc">-</div><div class="sub">lifetime avg</div></div>'
    '</div>'
    '<div class="cards" style="grid-template-columns:1.4fr 1fr">'
    '<div class="panel"><h3>Revenue by country</h3><canvas id="chartCountry" height="200"></canvas></div>'
    '<div class="panel"><h3>Top products <small>by revenue</small></h3><canvas id="chartProd" height="200"></canvas></div>'
    '</div>'
    '<div class="panel" style="margin-top:16px"><h3>Top customers <small>by lifetime value</small></h3>'
    '<table><thead><tr><th>Customer</th><th class="num">Lifetime value</th><th class="num">Orders</th><th class="num">Last order</th></tr></thead>'
    '<tbody id="custbody"><tr><td colspan="4" style="color:var(--muted)">waiting for data...</td></tr></tbody></table></div>'
    '</div>'
    "<script>" + _CHART_JS + """
const $=id=>document.getElementById(id);
const money=n=>(n==null)?"-":"\\u20b9"+Number(n).toLocaleString("en-IN",{maximumFractionDigits:0});
const num=n=>(n==null)?"-":Number(n).toLocaleString("en-IN",{maximumFractionDigits:2});
const esc=s=>String(s).replace(/[&<>]/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;"}[c]));
const initials=s=>(s||"?").split(" ").map(w=>w[0]).slice(0,2).join("").toUpperCase();
let cCountry,cProd;
function render(d){
  const o=d.overview||{};
  $("rev").textContent=money(o.total_revenue);$("ord").textContent=num(o.total_orders);
  $("aov").textContent=money(o.avg_order_value);$("cust").textContent=num(o.unique_customers);
  $("rpc").textContent=money(o.revenue_per_customer);
  $("ver").textContent="v"+d.version;$("upd").textContent="updated "+(d.generated_at||"");
  const c=d.country||[];
  if(!cCountry)cCountry=bar($("chartCountry"),false);
  cCountry.data.labels=c.map(x=>x.country);cCountry.data.datasets[0].data=c.map(x=>x.revenue);cCountry.update();
  const tp=(d.top_products||[]).slice(0,8);
  if(!cProd)cProd=bar($("chartProd"),true);
  cProd.data.labels=tp.map(x=>x.description||x.stock_code);cProd.data.datasets[0].data=tp.map(x=>x.revenue);cProd.update();
  const cu=d.customers||[];
  $("custbody").innerHTML=cu.length?cu.map((p,i)=>
    `<tr><td><span style="display:inline-flex;width:26px;height:26px;border-radius:50%;background:${PALETTE[i%PALETTE.length]};color:#fff;align-items:center;justify-content:center;font-size:11px;font-weight:600;margin-right:10px;vertical-align:middle">${initials(p.customer_name)}</span>${esc(p.customer_name??("#"+p.customer_id))}</td>`+
    `<td class="num">${money(p.lifetime_value)}</td><td class="num">${num(p.orders)}</td><td class="num">${esc(p.last_order_date??"-")}</td></tr>`).join(""):
    `<tr><td colspan="4" style="color:var(--muted)">no data yet</td></tr>`;
}
const es=new EventSource("/stream");
es.onmessage=e=>render(JSON.parse(e.data));
es.onerror=()=>{$("upd").textContent="reconnecting...";};
</script>""")


_CHAT_HTML = _page(
    "<header>" + _NAV.replace("__SUB__", "Ask the data - NL to SQL").replace(
        "__LINKS__", '<a class="chip" href="/">&larr; Dashboard</a><a class="chip" href="/quality">Data quality</a>')
    + "</header>"
    '<div class="wrap" style="max-width:880px;padding-bottom:120px"><div id="conv">'
    '<div style="color:var(--muted);text-align:center;margin-top:40px">Ask a question to get started, e.g. "revenue by country".</div>'
    '</div></div>'
    '<div class="barwrap"><div class="bar">'
    '<input id="q" style="flex:1" placeholder="Ask about revenue, products, customers, countries...  (press Enter)" autofocus/>'
    '<button onclick="ask()">Send</button></div></div>',
    extra_css=(
      ".barwrap{position:fixed;left:0;right:0;bottom:0;background:linear-gradient(180deg,rgba(245,246,250,0),var(--bg) 35%);padding:16px}"
      ".bar{max-width:880px;margin:0 auto;display:flex;gap:10px}.bar button{padding:0 22px}"
      ".turn{display:flex;flex-direction:column;gap:8px;margin-bottom:20px}"
      ".bubble{max-width:92%;border-radius:14px;padding:12px 14px}"
      ".user{align-self:flex-end;background:var(--accent-weak);color:#3730a3;font-weight:500}"
      ".bot{align-self:flex-start;background:var(--surface);border:1px solid var(--border);box-shadow:var(--shadow)}"
      ".user .meta{color:#6366f1;font-size:11px;margin-top:4px}"
      ".sql{font-family:ui-monospace,Menlo,monospace;font-size:12px;color:#0e7490;background:#f8fafc;"
      "border:1px solid var(--border);border-radius:8px;padding:8px 10px;margin:0 0 8px;white-space:pre-wrap;overflow:auto}"
      ".err{color:var(--red);font-size:13px}")
    + "<script>" + """
const $=id=>document.getElementById(id);
const esc=s=>String(s).replace(/[&<>]/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;"}[c]));
function rowsTable(rows){
  if(!rows||!rows.length)return '<div style="color:var(--muted)">No rows.</div>';
  const cols=Object.keys(rows[0]);
  return '<table><thead><tr>'+cols.map(c=>`<th>${esc(c)}</th>`).join("")+'</tr></thead><tbody>'+
    rows.map(r=>'<tr>'+cols.map(c=>`<td>${esc(r[c]??"")}</td>`).join("")+'</tr>').join("")+'</tbody></table>';
}
function render(hist){
  const el=$("conv");
  if(!hist||!hist.length){el.innerHTML='<div style="color:var(--muted);text-align:center;margin-top:40px">Ask a question to get started.</div>';return;}
  el.innerHTML=hist.map(h=>{
    const body=h.error?`<div class="err">${esc(h.error)}</div>`:`<div class="sql">${esc(h.sql)}</div>${rowsTable(h.rows)}`;
    return `<div class="turn"><div class="bubble user">${esc(h.q)}<div class="meta">${esc(h.ts||"")}</div></div><div class="bubble bot">${body}</div></div>`;
  }).join("");
  window.scrollTo(0,document.body.scrollHeight);
}
async function loadHistory(){try{const r=await fetch("/history");render((await r.json()).history);}catch(e){}}
async function ask(){const q=$("q").value.trim();if(!q)return;$("q").value="";await fetch("/ask?q="+encodeURIComponent(q));await loadHistory();}
$("q").addEventListener("keydown",e=>{if(e.key==="Enter"){e.preventDefault();ask();}});
loadHistory();
</script>""")


_QUALITY_HTML = _page(
    "<header>" + _NAV.replace("__SUB__", "Data quality - per-batch checks").replace(
        "__LINKS__", '<a class="chip" href="/">&larr; Dashboard</a><a class="chip" href="/chat">Ask the data</a>')
        .replace('<div class="badge ok"><span class="dot"></span>LIVE</div>',
                 '<div class="badge ok" id="status"><span class="dot"></span>OK</div>')
    + "</header>"
    '<div class="wrap">'
    '<div class="cards" style="grid-template-columns:repeat(4,1fr)">'
    '<div class="card"><div class="label">Rows checked (batch)</div><div class="val" id="batchRows">-</div></div>'
    '<div class="card"><div class="label">Rows checked (total)</div><div class="val" id="totalRows">-</div></div>'
    '<div class="card"><div class="label">Critical violations (total)</div><div class="val" id="crit">-</div></div>'
    '<div class="card"><div class="label">Batches checked</div><div class="val" id="batches">-</div></div>'
    '</div>'
    '<div class="tabs"><button class="tab active" id="tab-current" onclick="show(\'current\')">Current batch</button>'
    '<button class="tab" id="tab-overall" onclick="show(\'overall\')">Overall</button>'
    '<span class="tabmeta" id="meta">waiting for data...</span></div>'
    '<div id="view-current">'
    '<div class="panel" style="margin-bottom:16px"><h3>By dimension <small>rows failing, latest batch</small></h3>'
    '<div class="cards" id="dims-current" style="grid-template-columns:repeat(4,1fr);margin:0"></div></div>'
    '<div class="panel"><h3>Rules <small>latest batch</small></h3>'
    '<table><thead><tr><th>Check</th><th>Dimension</th><th>Pass rate</th><th class="num">Failed</th><th>Critical</th></tr></thead>'
    '<tbody id="rules-current"><tr><td colspan="5" style="color:var(--muted)">waiting for the first batch...</td></tr></tbody></table></div>'
    '</div>'
    '<div id="view-overall" style="display:none">'
    '<div class="panel" style="margin-bottom:16px"><h3>By dimension <small>rows failing, all batches</small></h3>'
    '<div class="cards" id="dims-overall" style="grid-template-columns:repeat(4,1fr);margin:0"></div></div>'
    '<div class="panel"><h3>Rules <small>cumulative across all batches</small></h3>'
    '<table><thead><tr><th>Check</th><th>Dimension</th><th>Pass rate</th><th class="num">Failed</th><th class="num">Checked</th><th>Critical</th></tr></thead>'
    '<tbody id="rules-overall"><tr><td colspan="6" style="color:var(--muted)">waiting...</td></tr></tbody></table></div>'
    '</div>'
    '</div>',
    extra_css=(
      ".crit{color:var(--amber);font-weight:600}"
      ".track{background:var(--line);border-radius:6px;height:8px;width:120px;display:inline-block;vertical-align:middle;margin-right:8px;overflow:hidden}"
      ".fill{display:block;height:100%;border-radius:6px;transition:width .3s ease}"
      ".tabs{display:flex;align-items:center;gap:8px;margin-bottom:16px}"
      ".tab{background:var(--surface);color:var(--muted);border:1px solid var(--border);border-radius:9px;padding:8px 16px;font-weight:600;font-size:13px;cursor:pointer}"
      ".tab.active{background:var(--accent);color:#fff;border-color:var(--accent)}"
      ".tabmeta{margin-left:auto;color:var(--muted);font-size:12px}")
    + "<script>" + """
const $=id=>document.getElementById(id);
const esc=s=>String(s).replace(/[&<>]/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;"}[c]));
const num=n=>(n==null)?"-":Number(n).toLocaleString("en-IN");
function color(p){return p>=99.5?"#16a34a":p>=95?"#d97706":"#dc2626";}
function show(which){
  for(const w of ["current","overall"]){
    $("view-"+w).style.display = (w===which)?"block":"none";
    $("tab-"+w).classList.toggle("active", w===which);
  }
}
function dims(el,bd){
  $(el).innerHTML=Object.keys(bd||{}).sort().map(k=>
    `<div class="card"><div class="label">${esc(k)}</div><div class="val">${num(bd[k].rows_failed)}</div><div class="sub">rows failing</div></div>`).join("")
    || '<div style="color:var(--muted)">-</div>';
}
function rulesTable(el,rules,showChecked){
  const rs=(rules||[]).slice().sort((a,b)=>a.dimension.localeCompare(b.dimension));
  $(el).innerHTML=rs.map(r=>{const p=r.passed_percent;
    const checked=showChecked?`<td class="num">${num(r.total)}</td>`:"";
    return `<tr><td>${esc(r.check)}::${esc(r.column)}</td><td>${esc(r.dimension)}</td>`+
      `<td><span class="track"><span class="fill" style="width:${p}%;background:${color(p)}"></span></span>${p}%</td>`+
      `<td class="num">${num(r.rows_failed)}</td>${checked}<td>${r.critical?'<span class="crit">critical</span>':''}</td></tr>`;}).join("")
    || '<tr><td colspan="6" style="color:var(--muted)">no data</td></tr>';
}
function render(dq){
  if(!dq)return;
  const cur=dq.current||{}, ov=dq.overall||{};
  $("batchRows").textContent=num(cur.row_count);
  $("totalRows").textContent=num(ov.rows_checked);
  $("crit").textContent=num(ov.critical_violations);
  $("batches").textContent=num(ov.batches);
  const fails=(cur.critical_failures||[]).length, st=$("status");
  st.className="badge "+(fails?"bad":"ok");
  st.innerHTML='<span class="dot"></span>'+(fails?("FAIL ("+fails+" critical)"):"OK");
  $("meta").textContent="batch "+dq.batch_id+" - "+(dq.generated_at||"");
  dims("dims-current",cur.by_dimension); rulesTable("rules-current",cur.rules,false);
  dims("dims-overall",ov.by_dimension); rulesTable("rules-overall",ov.rules,true);
}
const es=new EventSource("/stream");
es.onmessage=e=>{try{const d=JSON.parse(e.data);if(d.version!=null)$("ver").textContent="v"+d.version;if(d.generated_at)$("upd").textContent="updated "+d.generated_at;render(d.data_quality);}catch(x){}};
es.onerror=()=>{$("meta").textContent="reconnecting...";};
</script>""")
