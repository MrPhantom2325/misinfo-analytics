"""
dashboard/app.py — Phase 5: Real-Time Misinformation Analytics Dashboard
─────────────────────────────────────────────────────────────────────────
Panels:
  1. KPI Header Cards       — total articles, viral count, countries, avg cred
  2. Live Misinfo Feed      — scrolling table of latest flagged articles
  3. Category Breakdown     — animated donut chart by misinfo type
  4. Geo Spread Heatmap     — choropleth world map
  5. Viral Velocity Meter   — real-time line chart of share velocity
  6. Top Risky Domains      — horizontal bar chart, risk-scored
  7. Sentiment Timeline     — area chart of sentiment by category

Auto-refresh every 3 seconds via dcc.Interval.
Reads from SQLite DB written by Spark streaming jobs via storage.py.
"""

import os
import sys
import logging

import pandas as pd
import plotly.graph_objects as go
from dash import Dash, dcc, html, Input, Output

sys.path.insert(0, "/app")
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "spark"))
import storage

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

DB_PATH    = os.getenv("DB_PATH",  "/app/data/misinfo.db")
REFRESH_MS = 3000

# ─── Design tokens ─────────────────────────────────────────────────────────────

P = {
    "bg":      "#080b10",
    "surface": "#0e1219",
    "border":  "#1c2235",
    "accent":  "#e63946",
    "blue":    "#3a86ff",
    "amber":   "#f4a261",
    "teal":    "#2a9d8f",
    "text":    "#dde1ec",
    "muted":   "#5a6275",
    "grid":    "#141926",
}

CAT_COLORS = {
    "health":   "#e63946",
    "politics": "#3a86ff",
    "climate":  "#2a9d8f",
    "finance":  "#f4a261",
}

# Geo stream uses mostly ISO-2 country codes; choropleth expects ISO-3.
ISO2_TO_ISO3 = {
    "US": "USA", "RU": "RUS", "IN": "IND", "BR": "BRA", "GB": "GBR",
    "DE": "DEU", "CN": "CHN", "PH": "PHL", "MX": "MEX", "TR": "TUR",
    "PK": "PAK", "NG": "NGA", "ID": "IDN", "FR": "FRA", "IT": "ITA",
    "AU": "AUS", "UA": "UKR", "BY": "BLR", "CA": "CAN", "ES": "ESP",
    "NL": "NLD", "JP": "JPN", "KR": "KOR", "PL": "POL", "SE": "SWE",
    "NO": "NOR", "CH": "CHE", "ZA": "ZAF", "AR": "ARG", "CL": "CHL",
    "CO": "COL", "SA": "SAU", "AE": "ARE", "IR": "IRN", "IQ": "IRQ",
    "IL": "ISR", "EG": "EGY", "KE": "KEN", "ET": "ETH", "MA": "MAR",
    "DZ": "DZA", "TN": "TUN", "VN": "VNM", "TH": "THA", "MY": "MYS",
    "SG": "SGP", "NZ": "NZL", "BE": "BEL", "AT": "AUT", "PT": "PRT",
    "IE": "IRL", "DK": "DNK", "FI": "FIN", "CZ": "CZE", "RO": "ROU",
    "GR": "GRC", "HU": "HUN", "SK": "SVK", "HR": "HRV", "RS": "SRB",
}

FONT_MONO    = "'JetBrains Mono', 'Fira Code', monospace"
FONT_DISPLAY = "'Bebas Neue', 'Impact', sans-serif"
FONT_BODY    = "'IBM Plex Sans', 'DM Sans', sans-serif"

BASE_LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(color=P["text"], family=FONT_BODY, size=11),
    margin=dict(l=4, r=4, t=24, b=4),
    legend=dict(bgcolor="rgba(0,0,0,0)", bordercolor=P["border"],
                borderwidth=1, font=dict(size=10)),
    xaxis=dict(gridcolor=P["grid"], zerolinecolor=P["border"],
               linecolor=P["border"], tickfont=dict(size=9, color=P["muted"])),
    yaxis=dict(gridcolor=P["grid"], zerolinecolor=P["border"],
               linecolor=P["border"], tickfont=dict(size=9, color=P["muted"])),
)


# ─── Reusable components ────────────────────────────────────────────────────────

def card(children, extra=None):
    s = {"background": P["surface"], "border": f"1px solid {P['border']}",
         "borderRadius": "3px", "padding": "14px", "overflow": "hidden"}
    if extra:
        s.update(extra)
    return html.Div(children, style=s)


def tag(text, style=None):
    s = {"fontFamily": FONT_MONO, "fontSize": "9px", "letterSpacing": "2.5px",
         "color": P["muted"], "textTransform": "uppercase",
         "borderLeft": f"2px solid {P['border']}", "paddingLeft": "7px",
         "marginBottom": "10px"}
    if style:
        s.update(style)
    return html.Div(text, style=s)


def empty_fig(msg="awaiting stream..."):
    fig = go.Figure()
    fig.add_annotation(x=0.5, y=0.5, xref="paper", yref="paper",
                       text=msg, showarrow=False,
                       font=dict(color=P["muted"], size=12, family=FONT_MONO))
    fig.update_layout(**BASE_LAYOUT)
    return fig


def _to_iso3(code):
    """Normalize incoming country code/name to ISO-3 where possible."""
    if code is None:
        return None
    v = str(code).strip().upper()
    if len(v) == 3 and v.isalpha():
        return v
    if len(v) == 2:
        return ISO2_TO_ISO3.get(v)
    if v == "UK":
        return "GBR"
    return None


def cred_color(v):
    if v is None: return P["muted"]
    return P["accent"] if v < 0.15 else P["amber"] if v < 0.30 else P["teal"]


def kpi_card(label, value, color=None, sub=None):
    return card([
        html.Div(label, style={"fontFamily": FONT_MONO, "fontSize": "9px",
                               "letterSpacing": "2px", "color": P["muted"],
                               "textTransform": "uppercase", "marginBottom": "5px"}),
        html.Div(str(value), style={"fontFamily": FONT_DISPLAY, "fontSize": "30px",
                                    "letterSpacing": "1px", "color": color or P["text"],
                                    "lineHeight": "1"}),
        html.Div(sub or "", style={"fontFamily": FONT_MONO, "fontSize": "9px",
                                   "color": P["muted"], "marginTop": "3px"}),
    ], extra={"padding": "12px"})


# ─── App ───────────────────────────────────────────────────────────────────────

app = Dash(__name__, title="MISINFO MONITOR", update_title=None,
           meta_tags=[{"name": "viewport", "content": "width=device-width,initial-scale=1"}])

app.index_string = """<!DOCTYPE html>
<html><head>{%metas%}<title>{%title%}</title>{%favicon%}{%css%}
<style>
body,html{margin:0;padding:0;background:#080b10}
@keyframes blink{0%,100%{opacity:1;box-shadow:0 0 8px #e63946}50%{opacity:.3;box-shadow:none}}
@keyframes slideIn{from{opacity:0;transform:translateY(3px)}to{opacity:1;transform:none}}
::-webkit-scrollbar{width:3px}::-webkit-scrollbar-track{background:#080b10}
::-webkit-scrollbar-thumb{background:#1c2235;border-radius:2px}
</style></head>
<body>{%app_entry%}<footer>{%config%}{%scripts%}{%renderer%}</footer></body></html>"""


app.layout = html.Div(style={"background": P["bg"], "minHeight": "100vh",
                              "fontFamily": FONT_BODY, "color": P["text"]}, children=[

    # Google Fonts
    html.Link(rel="stylesheet",
              href="https://fonts.googleapis.com/css2?family=Bebas+Neue&family=IBM+Plex+Sans:wght@300;400;500&family=JetBrains+Mono:wght@400;500&display=swap"),


    # ── Header ─────────────────────────────────────────────────────────────────
    html.Div(style={"background": P["surface"], "borderBottom": f"1px solid {P['border']}",
                    "padding": "0 24px", "height": "50px", "display": "flex",
                    "alignItems": "center", "justifyContent": "space-between",
                    "position": "sticky", "top": "0", "zIndex": "100"}, children=[
        html.Div(style={"display": "flex", "alignItems": "center", "gap": "14px"}, children=[
            html.Div(style={"width": "7px", "height": "7px", "borderRadius": "50%",
                            "background": P["accent"], "animation": "blink 1.4s infinite",
                            "boxShadow": f"0 0 8px {P['accent']}"}),
            html.Span("MISINFO MONITOR", style={"fontFamily": FONT_DISPLAY,
                      "fontSize": "21px", "letterSpacing": "4px"}),
            html.Span("LIVE", style={"fontFamily": FONT_MONO, "fontSize": "9px",
                      "letterSpacing": "2px", "color": P["accent"],
                      "border": f"1px solid {P['accent']}", "padding": "2px 6px",
                      "borderRadius": "2px"}),
        ]),
        html.Div(id="clock", style={"fontFamily": FONT_MONO, "fontSize": "11px",
                                    "color": P["muted"], "letterSpacing": "1px"}),
    ]),

    # ── Main ───────────────────────────────────────────────────────────────────
    html.Div(style={"padding": "18px 24px", "maxWidth": "1640px", "margin": "0 auto"}, children=[

        # KPIs
        html.Div(id="kpis", style={"display": "grid", "gap": "10px",
                 "gridTemplateColumns": "repeat(6, 1fr)", "marginBottom": "14px"}),

        # Feed + donut
        html.Div(style={"display": "grid", "gap": "10px", "marginBottom": "14px",
                        "gridTemplateColumns": "1fr 330px"}, children=[
            card([tag("// live threat feed", {"borderLeftColor": P["accent"],
                      "color": P["accent"]}),
                  html.Div(id="feed")], extra={"minHeight": "320px"}),
            card([tag("// category breakdown"),
                  dcc.Graph(id="donut", config={"displayModeBar": False},
                            style={"height": "280px"})]),
        ]),

        # Geo + velocity
        html.Div(style={"display": "grid", "gap": "10px", "marginBottom": "14px",
                        "gridTemplateColumns": "1fr 400px"}, children=[
            card([tag("// geo spread heatmap"),
                  dcc.Graph(id="geo", config={"displayModeBar": False},
                            style={"height": "330px"})]),
            card([tag("// viral velocity meter", {"borderLeftColor": P["accent"],
                      "color": P["accent"]}),
                  dcc.Graph(id="velocity", config={"displayModeBar": False},
                            style={"height": "330px"})]),
        ]),

        # Domains + sentiment
        html.Div(style={"display": "grid", "gap": "10px",
                        "gridTemplateColumns": "1fr 1fr"}, children=[
            card([tag("// top risk domains"),
                  dcc.Graph(id="domains", config={"displayModeBar": False},
                            style={"height": "270px"})]),
            card([tag("// sentiment timeline"),
                  dcc.Graph(id="sentiment", config={"displayModeBar": False},
                            style={"height": "270px"})]),
        ]),
    ]),

    dcc.Interval(id="tick", interval=REFRESH_MS, n_intervals=0),
])


# ─── Callbacks ─────────────────────────────────────────────────────────────────

@app.callback(Output("clock", "children"), Input("tick", "n_intervals"))
def cb_clock(_):
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).strftime("UTC  %Y-%m-%d  %H:%M:%S")


@app.callback(Output("kpis", "children"), Input("tick", "n_intervals"))
def cb_kpis(_):
    s = storage.get_summary_stats(DB_PATH)
    c = s["avg_credibility"]
    return [
        kpi_card("Articles Seen",   f"{s['total_articles']:,}",     P["text"]),
        kpi_card("Viral Events",    f"{s['total_viral']:,}",        P["accent"],  "vel ≥ 5k"),
        kpi_card("Active Domains",  f"{s['active_domains']:,}",     P["amber"]),
        kpi_card("Countries Hit",   f"{s['countries_affected']:,}", P["blue"]),
        kpi_card("Avg Credibility", f"{c:.3f}",                     cred_color(c), "0=fake 1=real"),
        kpi_card("Top Category",    s["most_active_category"].upper(),
                 CAT_COLORS.get(s["most_active_category"], P["text"])),
    ]


@app.callback(Output("feed", "children"), Input("tick", "n_intervals"))
def cb_feed(_):
    df = storage.get_viral_articles(limit=14, db_path=DB_PATH)
    if df.empty:
        return html.Div("[ awaiting threat stream... ]",
                        style={"fontFamily": FONT_MONO, "color": P["muted"],
                               "fontSize": "12px", "padding": "24px 0"})

    cols = ["CAT", "GEO", "HEADLINE", "CRED", "VEL"]
    col_widths = "52px 52px 1fr 64px 68px"

    header = html.Div(style={"display": "grid", "gridTemplateColumns": col_widths,
                              "gap": "8px", "paddingBottom": "6px",
                              "borderBottom": f"1px solid {P['border']}",
                              "marginBottom": "2px"}, children=[
        html.Span(c, style={"fontFamily": FONT_MONO, "fontSize": "9px",
                            "letterSpacing": "2px", "color": P["muted"]}) for c in cols
    ])

    rows = []
    for _, r in df.iterrows():
        cat  = str(r.get("category", "?"))
        cred = r.get("credibility_score")
        vel  = int(r.get("share_velocity", 0))
        rows.append(html.Div(
            style={"display": "grid", "gridTemplateColumns": col_widths,
                   "gap": "8px", "padding": "7px 0",
                   "borderBottom": f"1px solid {P['border']}",
                   "alignItems": "center", "animation": "slideIn .25s ease"},
            children=[
                html.Span(cat[:5].upper(), style={
                    "fontFamily": FONT_MONO, "fontSize": "9px", "letterSpacing": "1px",
                    "color": CAT_COLORS.get(cat, P["muted"]),
                    "border": f"1px solid {CAT_COLORS.get(cat, P['border'])}",
                    "padding": "2px 4px", "borderRadius": "2px", "textAlign": "center"}),
                html.Span(str(r.get("geo_origin", "??")), style={
                    "fontFamily": FONT_MONO, "fontSize": "11px", "color": P["blue"]}),
                html.Span(str(r.get("headline", ""))[:88], style={
                    "fontSize": "11px", "color": P["text"],
                    "overflow": "hidden", "textOverflow": "ellipsis", "whiteSpace": "nowrap"}),
                html.Span(f"{cred:.2f}" if cred is not None else "—", style={
                    "fontFamily": FONT_MONO, "fontSize": "10px",
                    "color": cred_color(cred), "textAlign": "right"}),
                html.Span(f"↑{vel:,}", style={
                    "fontFamily": FONT_MONO, "fontSize": "10px", "textAlign": "right",
                    "color": P["accent"] if vel > 10000 else P["amber"]}),
            ],
        ))

    return [header] + rows


@app.callback(Output("donut", "figure"), Input("tick", "n_intervals"))
def cb_donut(_):
    df = storage.get_category_totals(DB_PATH)
    if df.empty:
        return empty_fig()

    colors = [CAT_COLORS.get(c, P["muted"]) for c in df["category"]]
    fig = go.Figure(go.Pie(
        labels=df["category"].str.upper(), values=df["total_count"],
        hole=0.58, marker=dict(colors=colors, line=dict(color=P["bg"], width=3)),
        textfont=dict(family=FONT_MONO, size=9),
        hovertemplate="<b>%{label}</b><br>%{value:,} articles<br>%{percent}<extra></extra>",
    ))
    total = int(df["total_count"].sum())
    fig.add_annotation(x=0.5, y=0.5, xref="paper", yref="paper", showarrow=False,
                       text=f"<b>{total:,}</b>",
                       font=dict(color=P["text"], size=16, family=FONT_MONO))
    layout = dict(**BASE_LAYOUT)
    layout["showlegend"] = True
    layout["legend"] = dict(bgcolor="rgba(0,0,0,0)", font=dict(size=10, family=FONT_MONO),
                            x=1.02, y=0.5, orientation="v")
    fig.update_layout(**layout)
    return fig


@app.callback(Output("geo", "figure"), Input("tick", "n_intervals"))
def cb_geo(_):
    df = storage.get_geo_heatmap(last_n_windows=10, db_path=DB_PATH)
    if df.empty:
        return empty_fig()

    df = df.copy()
    df["geo_iso3"] = df["geo_origin"].map(_to_iso3)
    df["total_articles"] = pd.to_numeric(df["total_articles"], errors="coerce")
    df = df[df["geo_iso3"].notna() & df["total_articles"].notna() & (df["total_articles"] > 0)]
    if df.empty:
        return empty_fig("no mappable geo data")

    fig = go.Figure(go.Choropleth(
        locations=df["geo_iso3"], z=df["total_articles"], locationmode="ISO-3",
        colorscale=[[0, "#0e1219"], [0.2, "#1c2235"], [0.5, "#3a86ff"],
                    [0.8, "#e63946"], [1, "#ff1a2e"]],
        colorbar=dict(title=dict(text="articles", font=dict(color=P["muted"], size=9)),
                      tickfont=dict(color=P["muted"], size=8), len=0.75, thickness=8,
                      bgcolor="rgba(0,0,0,0)", outlinecolor=P["border"]),
        marker=dict(line=dict(color=P["border"], width=0.3)),
        hovertemplate="<b>%{location}</b><br>Articles: %{z:,}<extra></extra>",
    ))
    fig.update_geos(showframe=False, showcoastlines=True, coastlinecolor=P["border"],
                    showland=True, landcolor="#0b0f18",
                    showocean=True, oceancolor="#080b10",
                    showlakes=False, bgcolor="rgba(0,0,0,0)",
                    projection_type="natural earth")
    layout = dict(**BASE_LAYOUT)
    layout["margin"] = dict(l=0, r=0, t=0, b=0)
    fig.update_layout(**layout)
    return fig


@app.callback(Output("velocity", "figure"), Input("tick", "n_intervals"))
def cb_velocity(_):
    df = storage.get_viral_velocity_series(last_n=40, db_path=DB_PATH)
    if df.empty:
        return empty_fig()

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df["minute_bucket"], y=df["avg_velocity"],
        fill="tozeroy", fillcolor="rgba(230,57,70,0.10)",
        line=dict(color=P["accent"], width=2), name="Avg",
        hovertemplate="%{x}<br>Avg: %{y:,.0f}<extra></extra>",
    ))
    fig.add_trace(go.Scatter(
        x=df["minute_bucket"], y=df["max_velocity"],
        line=dict(color=P["amber"], width=1, dash="dot"), name="Peak",
        hovertemplate="Peak: %{y:,.0f}<extra></extra>",
    ))
    fig.add_hline(y=5000, line_dash="dash", line_color=P["blue"],
                  line_width=1, opacity=0.5,
                  annotation_text="VIRAL THRESHOLD",
                  annotation_font=dict(color=P["blue"], size=8, family=FONT_MONO))
    layout = dict(**BASE_LAYOUT)
    layout["yaxis"] = dict(**BASE_LAYOUT["yaxis"], title="shares / min")
    layout["xaxis"] = dict(**BASE_LAYOUT["xaxis"], title=None, tickangle=-30)
    layout["showlegend"] = True
    fig.update_layout(**layout)
    return fig


@app.callback(Output("domains", "figure"), Input("tick", "n_intervals"))
def cb_domains(_):
    df = storage.get_top_risky_domains(top_n=10, db_path=DB_PATH)
    if df.empty:
        return empty_fig()

    df = df.copy()
    df["source_domain"] = df["source_domain"].fillna("(unknown)").astype(str)
    df["avg_risk_score"] = pd.to_numeric(df["avg_risk_score"], errors="coerce")
    df["total_articles"] = pd.to_numeric(df["total_articles"], errors="coerce").fillna(0)
    df["avg_credibility"] = pd.to_numeric(df["avg_credibility"], errors="coerce")
    df = df[df["avg_risk_score"].notna()]
    if df.empty:
        return empty_fig("no risk domain data")

    df = df.sort_values("avg_risk_score", ascending=True)
    max_risk = df["avg_risk_score"].max()
    denom = max_risk if pd.notna(max_risk) and max_risk > 0 else 1
    norm = df["avg_risk_score"] / denom
    colors = [f"rgba(230,57,70,{0.35 + 0.65 * v:.2f})" for v in norm]

    fig = go.Figure(go.Bar(
        x=df["avg_risk_score"], y=df["source_domain"], orientation="h",
        marker=dict(color=colors, line=dict(width=0)),
        customdata=df[["total_articles", "avg_credibility"]].values,
        hovertemplate="<b>%{y}</b><br>Risk: %{x:.1f}<br>Articles: %{customdata[0]:,}<br>Avg cred: %{customdata[1]:.3f}<extra></extra>",
    ))
    layout = dict(**BASE_LAYOUT)
    layout["xaxis"] = dict(**BASE_LAYOUT["xaxis"], title="Risk Score")
    y_tickfont = dict(BASE_LAYOUT["yaxis"].get("tickfont", {}))
    y_tickfont.update({"size": 9, "family": FONT_MONO})
    layout["yaxis"] = dict(**BASE_LAYOUT["yaxis"], tickfont=y_tickfont)
    layout["margin"] = dict(l=4, r=8, t=8, b=8)
    fig.update_layout(**layout)
    return fig


@app.callback(Output("sentiment", "figure"), Input("tick", "n_intervals"))
def cb_sentiment(_):
    df = storage.get_sentiment_timeline(last_n=60, db_path=DB_PATH)
    if df.empty:
        return empty_fig()

    fig = go.Figure()
    rgba_map = {
        "health":   "rgba(230,57,70,0.12)",
        "politics": "rgba(58,134,255,0.12)",
        "climate":  "rgba(42,157,143,0.12)",
        "finance":  "rgba(244,162,97,0.12)",
    }
    for cat, color in CAT_COLORS.items():
        cdf = df[df["category"] == cat].sort_values("window_start")
        if cdf.empty:
            continue
        fig.add_trace(go.Scatter(
            x=cdf["window_start"], y=cdf["avg_sentiment"],
            fill="tozeroy", fillcolor=rgba_map.get(cat, "rgba(255,255,255,0.05)"),
            line=dict(color=color, width=1.5), name=cat.upper(),
            hovertemplate=f"<b>{cat}</b><br>%{{x}}<br>Sentiment: %{{y:.3f}}<extra></extra>",
        ))
    fig.add_hline(y=0, line_color=P["muted"], line_width=0.6, opacity=0.5)
    layout = dict(**BASE_LAYOUT)
    layout["yaxis"] = dict(**BASE_LAYOUT["yaxis"], title="sentiment", range=[-1.1, 0.2])
    layout["xaxis"] = dict(**BASE_LAYOUT["xaxis"], title=None, tickangle=-30)
    layout["showlegend"] = True
    fig.update_layout(**layout)
    return fig


if __name__ == "__main__":
    port  = int(os.getenv("PORT", "8050"))
    debug = os.getenv("DEBUG", "false").lower() == "true"
    log.info(f"Dashboard starting → http://0.0.0.0:{port}")
    app.run(host="0.0.0.0", port=port, debug=debug)