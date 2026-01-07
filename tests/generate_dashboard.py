import os
import re
from datetime import datetime

# 1. Get the absolute path of the directory where THIS script is located (the 'tests' folder)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
COVERAGE_DIR = os.path.join(BASE_DIR, "coverage")
OUTPUT_FILE = os.path.join(COVERAGE_DIR, "index.html")


def parse_filename(filename):
    # Regex designed to handle the underscores in timestamps correctly
    pattern = r"heatmap_(?P<topic>[^_]+)_(?P<mode>[^_]+)_(?P<start>.+?)_to_(?P<end>.+?)_(?P<gran>[^_]+)_(?P<scope>[^_]+)\.html"
    match = re.match(pattern, filename)

    if match:
        d = match.groupdict()

        # Only include Fleet results
        if d['scope'].upper() != "FLEET":
            return None

        # UI Mapping
        d['display_mode'] = "New Table" if d['mode'].upper() == "FINAL" else "Old Table"

        # Clean Date Formatting (Month Year only)
        try:
            # We take just the first 8 chars (YYYYMMDD) for simplicity
            date_part = d['start'].split('_')[0]
            d['display_date'] = datetime.strptime(date_part, "%Y%m%d").strftime("%B %Y")
        except:
            d['display_date'] = "Data Report"

        d['filename'] = filename
        return d
    return None


def generate_html():
    print(f"--- Dashboard Debug ---")
    print(f"Searching in: {COVERAGE_DIR}")

    if not os.path.exists(COVERAGE_DIR):
        print(f"❌ ERROR: The directory {COVERAGE_DIR} does not exist.")
        return

    all_files = os.listdir(COVERAGE_DIR)
    print(f"Found {len(all_files)} total files in directory.")

    reports = []
    for f in all_files:
        if f.endswith(".html") and f != "index.html":
            data = parse_filename(f)
            if data:
                reports.append(data)
                print(f"✅ Matched: {f}")
            else:
                print(f"⚠️  Skipped (No Match): {f}")

    # Sort: Newest date first
    reports.sort(key=lambda x: (x['start']), reverse=True)

    html_template = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>asense IoT Coverage Dashboard</title>
        <script src="https://cdn.tailwindcss.com"></script>
        <style>
            body {{ background-color: #0f172a; color: #f8fafc; }}
            .card {{ background-color: #1e293b; transition: all 0.2s ease; border: 1px solid #334155; }}
            .card:hover {{ transform: translateY(-4px); border-color: #10b981; box-shadow: 0 10px 15px -3px rgba(0, 0, 0, 0.4); }}
        </style>
    </head>
    <body class="p-8 font-sans">
        <header class="max-w-6xl mx-auto mb-16 flex justify-between items-center">
            <div>
                <h1 class="text-4xl font-black text-white tracking-tight">IoT Coverage <span class="text-emerald-500">Explorer</span></h1>
                <p class="text-slate-400 mt-2 font-medium">Visualizing sensor uptime across fleet tables.</p>
            </div>
        </header>

        <main class="max-w-6xl mx-auto">
            <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-8">
                {"".join([f'''
                <a href="{r['filename']}" target="_blank" class="card p-8 rounded-3xl block group">
                    <div class="flex justify-between items-start mb-8">
                        <div class="px-3 py-1 rounded-lg text-[10px] font-black uppercase tracking-widest border 
                            {"border-emerald-500/50 text-emerald-400 bg-emerald-500/10" if r['display_mode'] == "New Table" else "border-slate-500/50 text-slate-400 bg-slate-500/10"}">
                            {r['display_mode']}
                        </div>
                        <div class="text-slate-600 font-mono text-[10px]">{r['gran']}</div>
                    </div>

                    <h2 class="text-4xl font-black text-white group-hover:text-emerald-400 transition-colors mb-2 uppercase">
                        {r['topic']}
                    </h2>
                    <p class="text-slate-400 font-bold tracking-tight mb-8">
                        {r['display_date']}
                    </p>

                    <div class="flex items-center text-white text-[10px] font-black uppercase tracking-[0.2em]">
                        View Report
                        <svg class="w-4 h-4 ml-2 group-hover:translate-x-2 transition-transform duration-300" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M17 8l4 4m0 0l-4 4m4-4H3"></path>
                        </svg>
                    </div>
                </a>
                ''' for r in reports])}
            </div>

            {f'<div class="text-center py-20 text-slate-500">No fleet reports found in {COVERAGE_DIR}.</div>' if not reports else ''}
        </main>

        <footer class="max-w-6xl mx-auto mt-32 pt-8 border-t border-slate-800 flex justify-between text-slate-600 text-[10px] uppercase font-bold tracking-widest">
            <div>ASENSE IoT Systems</div>
            <div>Generated: {datetime.now().strftime("%Y-%m-%d %H:%M")}</div>
        </footer>
    </body>
    </html>
    """

    with open(OUTPUT_FILE, "w") as f:
        f.write(html_template)
    print(f"--- Done ---")
    print(f"✅ Dashboard generated: {OUTPUT_FILE}")


if __name__ == "__main__":
    generate_html()