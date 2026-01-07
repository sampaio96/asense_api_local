import os
import re
from datetime import datetime

# Path to your coverage folder
COVERAGE_DIR = "coverage"
OUTPUT_FILE = "coverage/index.html"


def parse_filename(filename):
    # Pattern: heatmap_TOPIC_MODE_START_to_END_GRANmin_SCOPE.html
    pattern = r"heatmap_(?P<topic>.+?)_(?P<mode>.+?)_(?P<start>.+?)_to_(?P<end>.+?)_(?P<gran>.+?)_(?P<scope>.+?)\.html"
    match = re.match(pattern, filename)
    if match:
        d = match.groupdict()
        # Make the date look pretty
        d['start_pretty'] = datetime.strptime(d['start'], "%Y%m%d_%H%M%S").strftime("%b %Y")
        d['filename'] = filename
        return d
    return None


def generate_html():
    files = [f for f in os.listdir(COVERAGE_DIR) if f.endswith(".html") and f != "index.html"]
    reports = []

    for f in sorted(files, reverse=True):  # Newest first
        data = parse_filename(f)
        if data:
            reports.append(data)

    html_template = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>asense IoT Coverage Gallery</title>
        <script src="https://cdn.tailwindcss.com"></script>
        <style>
            body {{ background-color: #0f172a; color: #f8fafc; }}
            .card {{ background-color: #1e293b; transition: transform 0.2s; }}
            .card:hover {{ transform: translateY(-4px); background-color: #334155; }}
        </style>
    </head>
    <body class="p-8">
        <header class="max-w-6xl mx-auto mb-12">
            <h1 class="text-4xl font-bold text-emerald-400 mb-2">Coverage Analytics</h1>
            <p class="text-slate-400">Automated IoT sensor uptime heatmaps</p>
        </header>

        <main class="max-w-6xl mx-auto grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
            {"".join([f'''
            <a href="{r['filename']}" target="_blank" class="card p-6 rounded-xl border border-slate-700 block">
                <div class="flex justify-between items-start mb-4">
                    <span class="px-2 py-1 rounded text-xs font-bold uppercase tracking-wider 
                        {"bg-emerald-500/20 text-emerald-400" if r['mode'] == "FINAL" else "bg-amber-500/20 text-amber-400"}">
                        {r['mode']}
                    </span>
                    <span class="text-slate-500 text-sm">{r['gran']}</span>
                </div>
                <h2 class="text-2xl font-bold mb-1">{r['topic']}</h2>
                <p class="text-slate-400 text-sm mb-4">{r['start_pretty']}</p>
                <div class="text-emerald-500 text-sm font-semibold italic">View Heatmap →</div>
            </a>
            ''' for r in reports])}
        </main>

        <footer class="max-w-6xl mx-auto mt-20 pt-8 border-t border-slate-800 text-slate-500 text-sm">
            Last updated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
        </footer>
    </body>
    </html>
    """

    with open(OUTPUT_FILE, "w") as f:
        f.write(html_template)
    print(f"✅ Dashboard generated: {OUTPUT_FILE}")


if __name__ == "__main__":
    generate_html()