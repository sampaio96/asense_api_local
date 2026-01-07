import os
import re
from datetime import datetime

# Path to your coverage folder
COVERAGE_DIR = "coverage"
# Saving to root so the URL is shorter: sampaio96.github.io/asense_api_local/
OUTPUT_FILE = "coverage/index.html"


def parse_filename(filename):
    # Pattern: heatmap_TOPIC_MODE_START_to_END_GRANmin_SCOPE.html
    # Matches: heatmap_ACC_FINAL_20251201_000000_to_20251231_235959_60min_FLEET.html
    pattern = r"heatmap_(?P<topic>.+?)_(?P<mode>.+?)_(?P<start>.+?)_to_(?P<end>.+?)_(?P<gran>.+?)_(?P<scope>.+?)\.html"
    match = re.match(pattern, filename)

    if match:
        d = match.groupdict()

        # --- FILTER: ONLY MATCH FLEET ---
        if d['scope'].upper() != "FLEET":
            return None

        # Format the display date
        try:
            d['start_pretty'] = datetime.strptime(d['start'], "%Y%m%d_%H%M%S").strftime("%B %Y")
        except:
            d['start_pretty'] = d['start']

        # Path needs to point into the tests/coverage folder for the browser to find the files
        d['relative_path'] = f"tests/coverage/{filename}"
        return d
    return None


def generate_html():
    if not os.path.exists(COVERAGE_DIR):
        print(f"Error: {COVERAGE_DIR} not found.")
        return

    files = [f for f in os.listdir(COVERAGE_DIR) if f.endswith(".html")]
    reports = []

    for f in files:
        data = parse_filename(f)
        if data:
            reports.append(data)

    # Sort reports: Newest date first, then Final mode before Proto
    reports.sort(key=lambda x: (x['start'], x['mode']), reverse=True)

    html_template = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>asense IoT Fleet Coverage</title>
        <script src="https://cdn.tailwindcss.com"></script>
        <style>
            body {{ background-color: #0f172a; color: #f8fafc; font-family: ui-sans-serif, system-ui, sans-serif; }}
            .card {{ background-color: #1e293b; transition: all 0.2s ease-in-out; }}
            .card:hover {{ transform: translateY(-4px); background-color: #334155; border-color: #10b981; }}
        </style>
    </head>
    <body class="p-8">
        <header class="max-w-6xl mx-auto mb-12 flex justify-between items-end">
            <div>
                <h1 class="text-4xl font-extrabold text-emerald-400 mb-2">Fleet Coverage Dashboard</h1>
                <p class="text-slate-400">Heatmap explorer for high-frequency sensor tables</p>
            </div>
            <div class="text-right text-slate-500 text-xs uppercase tracking-widest">
                Last updated<br>
                <span class="text-slate-300 font-mono">{datetime.now().strftime("%Y-%m-%d %H:%M")}</span>
            </div>
        </header>

        <main class="max-w-6xl mx-auto">
            <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
                {"".join([f'''
                <a href="{r['relative_path']}" target="_blank" class="card p-6 rounded-2xl border border-slate-700 block shadow-xl">
                    <div class="flex justify-between items-center mb-6">
                        <span class="px-3 py-1 rounded-full text-[10px] font-black uppercase tracking-tighter 
                            {"bg-emerald-500/20 text-emerald-400 border border-emerald-500/30" if r['mode'] == "FINAL" else "bg-amber-500/20 text-amber-400 border border-amber-500/30"}">
                            {r['mode']}
                        </span>
                        <span class="text-slate-500 font-mono text-[10px]">{r['gran']}</span>
                    </div>
                    <div class="mb-4">
                        <h2 class="text-3xl font-black text-white leading-none mb-1">{r['topic'].upper()}</h2>
                        <p class="text-slate-400 font-medium">{r['start_pretty']}</p>
                    </div>
                    <div class="flex items-center text-emerald-500 text-xs font-bold uppercase tracking-widest group">
                        Open Heatmap 
                        <svg class="w-4 h-4 ml-2 transition-transform group-hover:translate-x-1" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M14 5l7 7m0 0l-7 7m7-7H3"></path>
                        </svg>
                    </div>
                </a>
                ''' for r in reports])}
            </div>

            {f'<div class="text-center py-20 text-slate-600 italic">No fleet reports found in {COVERAGE_DIR}.</div>' if not reports else ''}
        </main>
    </body>
    </html>
    """

    with open(OUTPUT_FILE, "w") as f:
        f.write(html_template)
    print(f"✅ Fleet Dashboard generated: {OUTPUT_FILE}")


if __name__ == "__main__":
    generate_html()