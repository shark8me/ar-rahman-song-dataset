import csv, os, collections

BASE = os.path.dirname(os.path.abspath(__file__))

songs = []
with open(os.path.join(BASE, 'data/recordings.csv')) as f:
    for row in csv.DictReader(f):
        date = (row.get('date') or '').strip()
        songs.append({'id': row['song_id'], 'title': row['song'], 'date': date, 'movie': row['movie']})

singers = collections.defaultdict(set)
with open(os.path.join(BASE, 'data/singers.csv')) as f:
    for row in csv.DictReader(f):
        singers[row['song_id']].add(row['singer'])

songs = [s for s in songs if len(s['date']) >= 4]
for s in songs:
    s['year'] = s['date'][:4]

def year_counts(fn):
    c = collections.Counter()
    for s in songs:
        c[fn(s)] += 1
    return c

per_year_songs = year_counts(lambda s: s['year'])
per_year_movies = collections.defaultdict(set)
per_year_singers = collections.defaultdict(set)
for s in songs:
    per_year_movies[s['year']].add(s['movie'])
    per_year_singers[s['year']] |= singers.get(s['id'], set())

all_years = sorted(set(range(1991, 2027)) & {int(s['year']) for s in songs})
ys = [str(y) for y in all_years]
songs_c = [per_year_songs.get(y, 0) for y in ys]
movies_c = [len(per_year_movies.get(y, ())) for y in ys]
singers_c = [len(per_year_singers.get(y, ())) for y in ys]

totals = {
    'songs': len(songs),
    'movies': len({s['movie'] for s in songs}),
    'singers': len({n for v in singers.values() for n in v}),
    'first': min(s['date'] for s in songs),
    'last': max(s['date'] for s in songs),
}
top = collections.Counter()
for sid, names in singers.items():
    for n in names:
        top[n] += 1

with open(os.path.join(BASE, 'stats.txt'), 'w') as f:
    f.write(repr({'totals': totals,
                  'top10': top.most_common(10),
                  'peak_songs': max(zip(songs_c, ys)),
                  'peak_movies': max(zip(movies_c, ys)),
                  'peak_singers': max(zip(singers_c, ys)),
                  'movies_c': dict(zip(ys, movies_c)),
                  'singers_c': dict(zip(ys, singers_c)),
                  'songs_c': dict(zip(ys, songs_c))}))

def bar_chart(fname, title, counts, years):
    W, H = 900, 320
    top_val = max(counts)
    steps = [top_val, top_val * 3 // 4 if False else round(top_val * 0.75),
             round(top_val * 0.5), round(top_val * 0.25), 0]
    def ypos(v):
        return 280 - 240 * v / top_val
    x0, x1 = 70, 890
    n = len(years)
    slot = (x1 - x0) / n
    bw = slot * 0.75
    out = [f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" font-family="Helvetica,Arial,sans-serif">']
    out.append(f'<text x="70" y="22" font-size="16" font-weight="bold" fill="#333">{title}</text>')
    for sy in (40, 100, 160, 220, 280):
        v = {40: steps[0], 100: steps[1], 160: steps[2], 220: steps[3], 280: 0}[sy]
        out.append(f'<line x1="70" y1="{sy}" x2="890" y2="{sy}" stroke="#ddd"/>')
        out.append(f'<text x="62" y="{sy+4}" font-size="11" fill="#666" text-anchor="end">{v}</text>')
    for i, (y, c) in enumerate(zip(years, counts)):
        x = x0 + i * slot + (slot - bw) / 2
        yy = ypos(c)
        h = 280 - yy
        if h < 0.1:
            yy, h = 269.6, 10.4 if c == 0 else h
            if c == 0:
                yy, h = 279.9, 0.1
        out.append(f'<rect x="{x:.1f}" y="{yy:.1f}" width="{bw:.1f}" height="{h:.1f}" fill="#4472c4"/>')
        if i % 3 == 0:
            out.append(f'<text x="{x0 + i*slot + slot/2:.0f}" y="300" font-size="11" fill="#666" text-anchor="middle">{y}</text>')
    out.append('<line x1="70" y1="280" x2="890" y2="280" stroke="#999"/>')
    out.append('</svg>')
    with open(os.path.join(BASE, 'doc/charts', fname), 'w') as f:
        f.write('\n'.join(out))

bar_chart('songs_per_year.svg', 'Songs released per year', songs_c, ys)
bar_chart('movies_per_year.svg', 'Albums / film soundtracks per year', movies_c, ys)
bar_chart('singers_per_year.svg', 'Distinct singers per year', singers_c, ys)
print(open(os.path.join(BASE, 'stats.txt')).read())