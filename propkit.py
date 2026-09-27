#!/usr/bin/env python3
"""
propkit.py - deterministic data + scoring + PDF pipeline for the NFL prop workflow.

  python3 propkit.py data   --away CIN --home PIT --season 2026 --week 3 [--work DIR]
  python3 propkit.py lines  --work DIR --api-key KEY [--book fanduel]    (optional, The Odds API)
  python3 propkit.py report --work DIR --lines lines.csv [--context context.json] [--out DIR]

`data` downloads nflverse files, computes team metrics/ranks/lane labels, in-scope players,
L10 windows, H2H, red-zone shares, and writes DIR/data.json + DIR/summary.txt (compact, for Claude)
+ DIR/lines_template.csv. `report` applies the 04/05/07 rules and builds the PDF.
"""
import argparse, json, math, os, re, subprocess, sys, datetime as dt
import warnings
warnings.filterwarnings('ignore')
import numpy as np
import pandas as pd

NV = "https://github.com/nflverse/nflverse-data/releases/download"
GAMES_URL = "https://raw.githubusercontent.com/nflverse/nfldata/master/data/games.csv"
CACHE = os.environ.get("PROPKIT_CACHE", "/tmp/propkit_cache")
WARN = []

def warn(msg):
    WARN.append(msg)
    print("WARN:", msg, file=sys.stderr)

# ----------------------------------------------------------------------------- download
def _get(url, fname):
    os.makedirs(CACHE, exist_ok=True)
    path = os.path.join(CACHE, fname)
    if not os.path.exists(path) or os.path.getsize(path) < 500:
        r = subprocess.run(["curl", "-sfL", "--max-time", "180", "-o", path, url])
        if r.returncode != 0:
            if os.path.exists(path):
                os.remove(path)
            raise RuntimeError(f"download failed: {url}")
    return path

def load(kind, season=None):
    try:
        if kind == "games":
            return pd.read_csv(_get(GAMES_URL, "games.csv"), low_memory=False)
        if kind == "players":
            return pd.read_csv(_get(f"{NV}/players/players.csv", "players.csv"), low_memory=False)
        paths = {
            "stats": (f"stats_player/stats_player_week_{season}.csv", f"sp{season}.csv"),
            "pbp": (f"pbp/play_by_play_{season}.parquet", f"pbp{season}.parquet"),
            "snaps": (f"snap_counts/snap_counts_{season}.csv", f"snap{season}.csv"),
            "roster": (f"rosters/roster_{season}.csv", f"roster{season}.csv"),
            "inj": (f"injuries/injuries_{season}.csv", f"inj{season}.csv"),
            "team": (f"stats_team/stats_team_week_{season}.csv", f"st{season}.csv"),
        }
        rel, fn = paths[kind]
        path = _get(f"{NV}/{rel}", fn)
        return pd.read_parquet(path) if fn.endswith("parquet") else pd.read_csv(path, low_memory=False)
    except Exception as e:
        warn(f"{kind} {season or ''} unavailable ({e})")
        return pd.DataFrame()

# ----------------------------------------------------------------------------- helpers
def r(x, n=1):
    try:
        if x is None or (isinstance(x, float) and math.isnan(x)):
            return None
        return round(float(x), n)
    except Exception:
        return None

def fmt(x, n=1, na="N/A"):
    v = r(x, n)
    if v is None:
        return na
    return f"{v:.{n}f}"

def norm_name(s):
    s = str(s).lower().replace(".", "").replace("'", "").replace("-", " ")
    s = re.sub(r"\b(jr|sr|ii|iii|iv|v)\b", "", s)
    return re.sub(r"\s+", " ", s).strip()

def tier_of(rank):
    if rank is None:
        return "N/A"
    return "Strength" if rank <= 10 else ("Weakness" if rank >= 23 else "Average")

MATRIX = {("Strength", "Weakness"): "ATTACK", ("Strength", "Average"): "LEAN ATTACK",
          ("Strength", "Strength"): "CONTESTED", ("Average", "Weakness"): "LEAN ATTACK",
          ("Average", "Average"): "NEUTRAL", ("Average", "Strength"): "LEAN FADE",
          ("Weakness", "Weakness"): "VOLATILE", ("Weakness", "Average"): "LEAN FADE",
          ("Weakness", "Strength"): "FADE"}
LABEL_ADJ = {"ATTACK": .15, "LEAN ATTACK": .075, "NEUTRAL": 0, "CONTESTED": -.05,
             "LEAN FADE": -.05, "FADE": -.15, "VOLATILE": 0, "N/A": 0}
# Positional receiving lanes (WR/TE/RB targets) use smaller, asymmetric adjustments. 2025 backtest:
# receivers facing bottom-10 positional defenses gained ~0 vs baseline, while top-10 defenses cut ~0.4-1.0 rec.
LABEL_ADJ_RECV = {"ATTACK": .05, "LEAN ATTACK": .025, "NEUTRAL": 0, "CONTESTED": -.05,
                  "LEAN FADE": -.05, "FADE": -.10, "VOLATILE": 0, "N/A": 0}
RECV_LANES = {"WR targets", "TE targets", "RB receiving"}

# lane name, metric key (same key for offense and defense tables)
LANES = [("Pass efficiency", "ypa"), ("WR targets", "WR_yds_pg"), ("TE targets", "TE_yds_pg"),
         ("RB receiving", "RB_yds_pg"), ("Explosives/depth", "exp_pass_rate"), ("Run game", "ypc"),
         ("Protection vs rush", "sack_rate"), ("Points per 100 yds", "pts_per_100"),
         ("Red zone TD %", "rz_td_pct"), ("3rd down", "third_pct")]
LOWER_BETTER_OFF = {"sack_rate", "stuff_rate", "int_rate"}
HIGHER_BETTER_DEF = {"sack_rate", "stuff_rate"}

# ----------------------------------------------------------------------------- team metrics
def team_metrics(pbp, games, season, pos_map):
    p = pbp[(pbp.season == season) & (pbp.season_type == "REG")].copy()
    if p.empty:
        return {}
    pl = p[p.play_type.isin(["pass", "run"]) & (p.two_point_attempt.fillna(0) == 0)].copy()
    att = pl[(pl.pass_attempt == 1) & (pl.sack == 0)].copy()
    att["rpos"] = att.receiver_player_id.map(pos_map).replace({"FB": "RB", "HB": "RB"})
    runs = pl[(pl.play_type == "run") & (pl.qb_scramble.fillna(0) == 0)]
    rushes_all = pl[pl.play_type == "run"].copy()
    rushes_all["rpos"] = rushes_all.rusher_player_id.map(pos_map).replace({"FB": "RB", "HB": "RB"})
    sacks = pl[pl.sack == 1]
    db = pl[pl.qb_dropback == 1]
    neutral = pl[(pl.wp.between(.2, .8)) & (pl.qtr <= 3)]
    g_sc = games[(games.season == season) & (games.game_type == "REG") & games.home_score.notna()]
    pts_for, pts_against = {}, {}
    for _, gm in g_sc.iterrows():
        for t, pf, pa in [(gm.home_team, gm.home_score, gm.away_score), (gm.away_team, gm.away_score, gm.home_score)]:
            pts_for[t] = pts_for.get(t, 0) + pf
            pts_against[t] = pts_against.get(t, 0) + pa
    drives = p[p.fixed_drive.notna() & p.posteam.notna()].groupby(["game_id", "posteam", "defteam", "fixed_drive"]).agg(
        minyl=("yardline_100", "min"), res=("fixed_drive_result", "first"),
        top=("drive_time_of_possession", "first")).reset_index()
    drives["rz"] = drives.minyl <= 20
    drives["td"] = drives.res == "Touchdown"

    def top_sec(s):
        try:
            m, sec = str(s).split(":")
            return int(m) * 60 + int(sec)
        except Exception:
            return np.nan
    drives["top_s"] = drives.top.map(top_sec)
    out = {}
    for side, key in [("off", "posteam"), ("def", "defteam")]:
        g = pl.groupby(key).game_id.nunique()
        m = pd.DataFrame(index=g.index)
        m["games"] = g
        m["plays_pg"] = pl.groupby(key).size() / g
        a = att.groupby(key)
        m["ypa"] = a.yards_gained.sum() / a.size()
        m["comp_pct"] = a.complete_pass.mean() * 100
        m["cpoe"] = a.cpoe.mean()
        m["adot"] = a.air_yards.mean()
        m["exp_pass_rate"] = (att.yards_gained >= 20).groupby(att[key]).mean() * 100
        ptd = a.pass_touchdown.sum()
        ints = a.interception.sum()
        sy = sacks.groupby(key).yards_gained.sum().reindex(m.index).fillna(0)
        ns = sacks.groupby(key).size().reindex(m.index).fillna(0)
        m["anya"] = (a.yards_gained.sum() + 20 * ptd - 45 * ints + sy) / (a.size() + ns)
        m["int_rate"] = ints / a.size() * 100
        tot_rec = a.yards_gained.sum()
        for pos in ["WR", "TE", "RB"]:
            sub = att[att.rpos == pos].groupby(key)
            m[f"{pos}_yds_pg"] = sub.yards_gained.sum() / g
            m[f"{pos}_tgt_pg"] = sub.size() / g
            m[f"{pos}_rec_pg"] = sub.complete_pass.sum() / g
            m[f"{pos}_catch"] = sub.complete_pass.mean() * 100
            m[f"{pos}_yds_share"] = sub.yards_gained.sum() / tot_rec * 100
            m[f"{pos}_rec_td_pg"] = sub.pass_touchdown.sum() / g
        m["RB_rush_td_pg"] = rushes_all[rushes_all.rpos == "RB"].groupby(key).rush_touchdown.sum() / g
        rg = runs.groupby(key)
        m["ypc"] = rg.yards_gained.mean()
        m["exp_run_rate"] = (runs.yards_gained >= 10).groupby(runs[key]).mean() * 100
        m["stuff_rate"] = (runs.yards_gained <= 0).groupby(runs[key]).mean() * 100
        m["sack_rate"] = ns / db.groupby(key).size() * 100
        m["dropback_rate"] = db.groupby(key).size() / pl.groupby(key).size() * 100
        m["scramble_rate"] = db.groupby(key).qb_scramble.mean() * 100
        yds = pl.groupby(key).yards_gained.sum()
        m["yds_pg"] = yds / g
        pts = pd.Series(pts_for if side == "off" else pts_against)
        m["pts_pg"] = pts.reindex(m.index) / g
        m["pts_per_100"] = pts.reindex(m.index) / yds * 100
        dg = drives.groupby(key)
        m["rz_trips_pg"] = dg.rz.sum() / g
        m["rz_td_pct"] = drives[drives.rz].groupby(key).td.mean() * 100
        third = p[p.down == 3]
        tc = third.groupby(key).third_down_converted.sum()
        tf = third.groupby(key).third_down_failed.sum()
        m["third_pct"] = tc / (tc + tf) * 100
        m["neutral_pass_rate"] = neutral.groupby(key).qb_dropback.mean() * 100
        xp = pl[pl.xpass.notna()]
        m["proe"] = (xp.groupby(key)["pass"].mean() - xp.groupby(key).xpass.mean()) * 100
        m["fg_pg"] = p[p.field_goal_result == "made"].groupby(key).size().reindex(m.index).fillna(0) / g
        m["rush_td_pg"] = pl.groupby(key).rush_touchdown.sum() / g
        m["pass_td_pg"] = pl.groupby(key).pass_touchdown.sum() / g
        if side == "off":
            m["top_min_pg"] = dg.top_s.sum() / g / 60
            s = neutral.sort_values(["game_id", "play_id"])
            s = s.assign(d=s.groupby(["game_id", "fixed_drive"]).game_seconds_remaining.diff(-1))
            s = s[(s.d > 0) & (s.d < 60)]
            m["pace_sec"] = s.groupby(key).d.mean()
        out[side] = m
    return out

def blend_metrics(cur, prior, k=6):
    """Blend current and prior season team tables. weight_cur = g/(g+k)."""
    res = {}
    for side in ["off", "def"]:
        c = cur.get(side) if cur else None
        pr = prior.get(side) if prior else None
        if c is None and pr is None:
            continue
        if c is None:
            b = pr.copy(); w = pd.Series(0.0, index=b.index)
        elif pr is None:
            b = c.copy(); w = pd.Series(1.0, index=b.index)
        else:
            idx = c.index.union(pr.index)
            c2, p2 = c.reindex(idx), pr.reindex(idx)
            w = (c2.games / (c2.games + k)).fillna(0)
            b = c2.mul(w, axis=0).add(p2.mul(1 - w, axis=0), fill_value=np.nan)
            b = b.where(~c2.isna(), p2).where(~p2.isna(), c2)
        b["w_cur"] = w
        # ranks: 1 = best
        for col in b.columns:
            if col in ("games", "w_cur"):
                continue
            if side == "off":
                asc = col in LOWER_BETTER_OFF
            else:
                asc = col not in HIGHER_BETTER_DEF
            b[col + "_rank"] = b[col].rank(ascending=asc, method="min")
        res[side] = b
    return res

# ----------------------------------------------------------------------------- data command
POS_KEEP = {"QB", "RB", "WR", "TE", "FB"}

def cmd_data(a):
    S, W = a.season, a.week
    away, home = a.away.upper(), a.home.upper()
    os.makedirs(a.work, exist_ok=True)
    games = load("games")
    gm = games[(games.season == S) & (games.week == W) & (games.away_team == away) & (games.home_team == home)]
    if gm.empty:
        gm = games[(games.season == S) & (games.week == W) & (((games.away_team == home) & (games.home_team == away)))]
        if gm.empty:
            print(f"STOP: no {away} at {home} game found in {S} week {W}.")
            sys.exit(2)
        away, home = home, away
    gm = gm.iloc[0]
    seasons = [S - 2, S - 1, S]
    stats = pd.concat([load("stats", s) for s in seasons], ignore_index=True)
    stats = stats[~((stats.season == S) & (stats.week >= W))]
    pbp = pd.concat([load("pbp", s) for s in [S - 1, S]], ignore_index=True)
    if not pbp.empty:
        pbp = pbp[~((pbp.season == S) & (pbp.week >= W))]
    snaps = pd.concat([load("snaps", s) for s in seasons], ignore_index=True)
    if not snaps.empty:
        snaps = snaps[~((snaps.season == S) & (snaps.week >= W))]
    rosters = pd.concat([load("roster", s) for s in [S - 1, S]], ignore_index=True)
    inj = load("inj", S)
    players = load("players")
    pos_map = {}
    if not players.empty:
        pos_map.update(players.set_index("gsis_id").position.dropna().to_dict())
    if not rosters.empty:
        pos_map.update(rosters.dropna(subset=["gsis_id"]).drop_duplicates("gsis_id", keep="last").set_index("gsis_id").position.to_dict())
    pfr_map = players.dropna(subset=["gsis_id", "pfr_id"]).set_index("gsis_id").pfr_id.to_dict() if not players.empty else {}

    # ---------- team metrics
    tm_cur = team_metrics(pbp, games, S, pos_map) if not pbp.empty else {}
    tm_pri = team_metrics(pbp, games, S - 1, pos_map) if not pbp.empty else {}
    tb = blend_metrics(tm_cur, tm_pri)

    def team_block(t):
        blk = {}
        for side in ["off", "def"]:
            d = {}
            for col in tb[side].columns:
                if col.endswith("_rank") or col in ("w_cur",):
                    continue
                v = tb[side].at[t, col] if t in tb[side].index else np.nan
                rk = tb[side].at[t, col + "_rank"] if (col + "_rank") in tb[side].columns and t in tb[side].index else np.nan
                cv = tm_cur.get(side).at[t, col] if tm_cur and t in tm_cur.get(side).index else np.nan
                pv = tm_pri.get(side).at[t, col] if tm_pri and t in tm_pri.get(side).index else np.nan
                d[col] = {"v": r(v, 2), "rank": None if pd.isna(rk) else int(rk), "cur": r(cv, 2), "prior": r(pv, 2)}
            d["w_cur"] = r(tb[side].at[t, "w_cur"], 2) if t in tb[side].index else None
            blk[side] = d
        return blk
    teams = {away: team_block(away), home: team_block(home)}
    lg = {side: {c: r(tb[side][c].mean(), 2) for c in tb[side].columns if not c.endswith("_rank")} for side in tb}

    def lanes(off_t, def_t):
        out = []
        for lane, key in LANES:
            o, d = teams[off_t]["off"].get(key, {}), teams[def_t]["def"].get(key, {})
            ot, dtier = tier_of(o.get("rank")), tier_of(d.get("rank"))
            label = MATRIX.get((ot, dtier), "N/A")
            out.append({"lane": lane, "key": key, "off": o.get("v"), "off_rank": o.get("rank"),
                        "def": d.get("v"), "def_rank": d.get("rank"), "off_tier": ot, "def_tier": dtier, "label": label})
        return out
    matchups = {away: lanes(away, home), home: lanes(home, away)}

    # ---------- player game tables
    sp = stats.copy()
    sp["position"] = sp.position.replace({"FB": "RB", "HB": "RB"})
    sp = sp[sp.position.isin(["QB", "RB", "WR", "TE"])]
    team_game = stats.groupby(["game_id", "team"]).agg(team_tgt=("targets", "sum"), team_car=("carries", "sum"),
                                                        team_att=("attempts", "sum")).reset_index()
    qb_game = stats.sort_values("attempts", ascending=False).drop_duplicates(["game_id", "team"])[["game_id", "team", "player_id", "player_display_name"]]
    qb_game.columns = ["game_id", "team", "qb_id", "qb_name"]
    # longest & red zone from pbp
    longest = pd.DataFrame()
    rz = pd.DataFrame()
    if not pbp.empty:
        comp = pbp[(pbp.complete_pass == 1) & (pbp.sack == 0)]
        lr = comp.groupby(["game_id", "receiver_player_id"]).yards_gained.max().reset_index()
        lr.columns = ["game_id", "player_id", "long_rec"]
        lc = comp.groupby(["game_id", "passer_player_id"]).yards_gained.max().reset_index()
        lc.columns = ["game_id", "player_id", "long_comp"]
        longest = lr.merge(lc, on=["game_id", "player_id"], how="outer")
        base = pbp[pbp.play_type.isin(["pass", "run"]) & (pbp.two_point_attempt.fillna(0) == 0) & (pbp.yardline_100 <= 20)]
        ru = base[base.play_type == "run"]
        tg = base[(base.pass_attempt == 1) & (base.sack == 0) & base.receiver_player_id.notna()]
        def agg(df, idcol, pre):
            x = df.assign(i10=df.yardline_100 <= 10, i5=df.yardline_100 <= 5).groupby(["game_id", "posteam", idcol]).agg(
                **{f"{pre}20": ("play_id", "size"), f"{pre}10": ("i10", "sum"), f"{pre}5": ("i5", "sum")}).reset_index()
            return x.rename(columns={idcol: "player_id", "posteam": "team"})
        rz = agg(ru, "rusher_player_id", "rzc").merge(agg(tg, "receiver_player_id", "rzt"), on=["game_id", "team", "player_id"], how="outer").fillna(0)
        team_rz = rz.groupby(["game_id", "team"])[["rzc20", "rzc10", "rzc5", "rzt20", "rzt10", "rzt5"]].sum().add_prefix("team_").reset_index()
    snaps_by = {}
    if not snaps.empty:
        snaps_by = {k: v for k, v in snaps.groupby("pfr_player_id")}

    def player_games(pid, pos, name):
        g = sp[sp.player_id == pid].copy()
        pfr = pfr_map.get(pid)
        sn = snaps_by.get(pfr, pd.DataFrame())
        if not sn.empty:
            sn = sn[sn.offense_snaps > 0][["game_id", "team", "season", "week", "offense_pct", "opponent"]]
            missing = sn[~sn.game_id.isin(g.game_id)]
            if len(missing):
                add = missing.rename(columns={"opponent": "opponent_team"}).copy()
                add["player_id"] = pid
                g = pd.concat([g, add.drop(columns=["offense_pct"])], ignore_index=True)
            g = g.merge(sn[["game_id", "offense_pct"]], on="game_id", how="left")
        else:
            g["offense_pct"] = np.nan
        num = ["attempts", "completions", "passing_yards", "passing_tds", "passing_interceptions", "carries",
               "rushing_yards", "rushing_tds", "targets", "receptions", "receiving_yards", "receiving_tds", "sacks_suffered"]
        for c in num:
            g[c] = g[c].fillna(0)
        g = g.merge(team_game, on=["game_id", "team"], how="left").merge(qb_game, on=["game_id", "team"], how="left")
        if not longest.empty:
            g = g.merge(longest, on=["game_id", "player_id"], how="left")
        else:
            g["long_rec"] = np.nan; g["long_comp"] = np.nan
        if not rz.empty:
            g = g.merge(rz, on=["game_id", "team", "player_id"], how="left").merge(team_rz, on=["game_id", "team"], how="left")
            has_pbp = g.game_id.isin(pbp.game_id.unique())
            for c in ["rzc20", "rzc10", "rzc5", "rzt20", "rzt10", "rzt5"]:
                g[c] = g[c].where(~has_pbp, g[c].fillna(0))
            if pos in ("WR", "TE", "RB"):
                g["long_rec"] = g.long_rec.where(~has_pbp | (g.receptions == 0), g.long_rec).where(~(has_pbp & (g.receptions == 0)), 0)
        g = g.sort_values(["season", "week"], ascending=False).drop_duplicates("game_id")
        g["season_type"] = g.season_type.fillna("REG")
        g.loc[g.week >= 19, "season_type"] = "POST"
        # injury-exit exclusion: snap% < 50% of median of recent 20
        med = g.head(20).offense_pct.median()
        g["excluded"] = False
        if not pd.isna(med):
            g["excluded"] = g.offense_pct < 0.5 * med
        g["pass_yds"] = g.passing_yards; g["pass_att"] = g.attempts; g["cmp"] = g.completions
        g["pass_td"] = g.passing_tds; g["int"] = g.passing_interceptions
        g["rush_att"] = g.carries; g["rush_yds"] = g.rushing_yards; g["rec"] = g.receptions; g["rec_yds"] = g.receiving_yards
        g["rush_rec_yds"] = g.rush_yds + g.rec_yds
        g["td"] = g.rushing_tds + (0 if pos == "QB" else g.receiving_tds)
        g["snap_pct"] = g.offense_pct * 100 if g.offense_pct.max() <= 1.5 else g.offense_pct
        return g

    METRICS = {"QB": ["pass_att", "cmp", "pass_yds", "pass_td", "int", "rush_att", "rush_yds", "long_comp", "td"],
               "RB": ["rush_att", "rush_yds", "targets", "rec", "rec_yds", "rush_rec_yds", "td"],
               "WR": ["targets", "rec", "rec_yds", "long_rec", "td"],
               "TE": ["targets", "rec", "rec_yds", "long_rec", "td"]}

    # ---------- injuries
    inj_rows = {}
    if not inj.empty:
        iw = inj[(inj.season == S) & (inj.week == W) & inj.team.isin([away, home])]
        for _, x in iw.iterrows():
            inj_rows[x.gsis_id] = {"name": x.full_name, "team": x.team, "pos": x.position,
                                   "status": x.report_status if isinstance(x.report_status, str) else "None",
                                   "practice": x.practice_status if isinstance(x.practice_status, str) else "",
                                   "injury": x.report_primary_injury if isinstance(x.report_primary_injury, str) else ""}

    # ---------- in-scope selection
    def select(team, qb_id):
        cur_ros = rosters[(rosters.season == S)] if not rosters.empty else pd.DataFrame()
        if not cur_ros.empty and "week" in cur_ros.columns:
            cur_ros = cur_ros.sort_values("week").drop_duplicates("gsis_id", keep="last")
        ids = set(cur_ros[(cur_ros.team == team) & (cur_ros.position.isin(POS_KEEP))].gsis_id) if not cur_ros.empty else set()
        ids |= set(sp[(sp.season == S) & (sp.team == team)].player_id)
        ids |= {k for k, v in inj_rows.items() if v["team"] == team and v["pos"] in POS_KEEP}
        # drop players whose latest game is for another team this season
        last = sp.sort_values(["season", "week"]).drop_duplicates("player_id", keep="last").set_index("player_id")
        cands = []
        for pid in ids:
            if pid in last.index and last.at[pid, "season"] == S and last.at[pid, "team"] != team:
                continue
            pos = pos_map.get(pid)
            pos = "RB" if pos in ("FB", "HB") else pos
            if pos not in ("QB", "RB", "WR", "TE"):
                continue
            name = last.at[pid, "player_display_name"] if pid in last.index else inj_rows.get(pid, {}).get("name", pid)
            g = player_games(pid, pos, name)
            valid = g[~g.excluded]
            l10 = valid.head(10)
            cur = valid[(valid.season == S) & (valid.team == team)]
            def pg(df, c):
                return df[c].mean() if len(df) else 0
            wc = len(cur) / (len(cur) + 3.0)
            if pos in ("WR", "TE"):
                score = wc * pg(cur, "targets") + (1 - wc) * pg(l10, "targets")
            elif pos == "RB":
                opp = lambda d: (d.carries + d.targets).mean() if len(d) else 0
                score = wc * opp(cur) + (1 - wc) * opp(l10)
            else:
                score = wc * pg(cur, "attempts") + (1 - wc) * pg(l10, "attempts")
            snap = cur.snap_pct.mean() if len(cur) else l10.snap_pct.mean()
            st = inj_rows.get(pid, {}).get("status", "None")
            cands.append({"id": pid, "name": name, "pos": pos, "score": score, "snap": snap, "status": st, "g": g})
        cands.sort(key=lambda c: -c["score"])
        chosen, vacated = [], []
        def take(pos, n, cond=lambda c: True):
            k = 0
            for c in [c for c in cands if c["pos"] == pos]:
                if k >= n:
                    break
                if not cond(c):
                    continue
                if c["status"] in ("Out", "Doubtful"):
                    vacated.append(c); continue
                chosen.append(c); k += 1
        qbs = [c for c in cands if c["pos"] == "QB"]
        qb = next((c for c in qbs if c["id"] == qb_id), None) or (qbs[0] if qbs else None)
        if qb:
            chosen.append(qb)
        take("RB", 1)
        rb2 = [c for c in cands if c["pos"] == "RB" and c not in chosen and c not in vacated and c["status"] not in ("Out", "Doubtful")]
        if rb2 and (rb2[0]["snap"] or 0) >= 30:
            chosen.append(rb2[0])
        take("WR", 3)
        take("TE", 1)
        te2 = [c for c in cands if c["pos"] == "TE" and c not in chosen and c not in vacated and c["status"] not in ("Out", "Doubtful")]
        if te2 and (te2[0]["snap"] or 0) >= 40:
            chosen.append(te2[0])
        for c in cands:
            if c["status"] in ("Out", "Doubtful") and c not in vacated and c["pos"] != "QB":
                vacated.append(c)
        return chosen, vacated

    def summarize(c, team, cur_qb_id):
        g = c["g"]
        valid = g[~g.excluded]
        l10 = valid.head(10).copy()
        excl = g[g.excluded & (g.index < (valid.head(10).index.max() if len(valid) else 0) + 1)].head(3)
        n_cur = int((l10.season == S).sum())
        split = f"{n_cur} ({S}) + {len(l10) - n_cur} ({S - 1}{'/' + str(S - 2) if (l10.season == S - 2).any() else ''})"
        prior = l10[l10.season < S]
        cur_team = valid[(valid.season == S) & (valid.team == team)]
        flags = []
        if len(prior) and (prior.team != team).mean() > 0.5:
            flags.append("team change")
        if len(prior) and cur_qb_id and c["pos"] != "QB" and (prior.qb_id != cur_qb_id).mean() > 0.5:
            flags.append("QB change")
        w = np.where(l10.season == S, 2.0 if flags else 1.0, 1.0)
        mets = {}
        for m in METRICS[c["pos"]]:
            s = l10[m].dropna() if m in l10.columns else pd.Series(dtype=float)
            if s.empty:
                mets[m] = None; continue
            ww = w[l10[m].notna().values]
            avg = s.mean(); wavg = np.average(s, weights=ww)
            l3 = s.head(3).mean()
            small = avg < 5
            thr = 0.5 if small else 0.10 * avg
            trend = "Up" if l3 - avg > thr else ("Down" if avg - l3 > thr else "Flat")
            mets[m] = {"avg": r(avg, 2), "wavg": r(wavg, 2), "med": r(s.median(), 2), "hi": r(s.max(), 1), "lo": r(s.min(), 1),
                       "trend": trend, "n": int(len(s)), "std": r(s.std(ddof=0), 2), "games": [r(x, 1) for x in s.tolist()]}
        # shares / rates (weighted ratio of sums)
        def wsum(col):
            return float(np.nansum(l10[col].values * w)) if col in l10 else 0.0
        rates = {
            "target_share": wsum("targets") / wsum("team_tgt") if wsum("team_tgt") else None,
            "carry_share": wsum("carries") / wsum("team_car") if wsum("team_car") else None,
            "catch_rate": wsum("receptions") / wsum("targets") if wsum("targets") else None,
            "yds_per_tgt": wsum("receiving_yards") / wsum("targets") if wsum("targets") else None,
            "ypc": wsum("rushing_yards") / wsum("carries") if wsum("carries") else None,
            "comp_pct": wsum("completions") / wsum("attempts") if wsum("attempts") else None,
            "yds_per_att": wsum("passing_yards") / wsum("attempts") if wsum("attempts") else None,
            "td_rate": wsum("passing_tds") / wsum("attempts") if wsum("attempts") else None,
            "int_rate": wsum("passing_interceptions") / wsum("attempts") if wsum("attempts") else None,
            "snap_pct": r(l10.snap_pct.mean(), 1),
            "tgt_per_att": wsum("team_tgt") / wsum("team_att") if wsum("team_att") else 0.93,
        }
        # red zone (games with pbp)
        rzg = l10[l10.get("team_rzc20", pd.Series(dtype=float)).notna()] if "team_rzc20" in l10 else l10.iloc[0:0]
        rzd = {}
        if len(rzg):
            pc = (rzg.rzc20 + rzg.rzc10).sum(); tc = (rzg.team_rzc20 + rzg.team_rzc10).sum()
            pt = (rzg.rzt20 + rzg.rzt10).sum(); tt = (rzg.team_rzt20 + rzg.team_rzt10).sum()
            K = 8.0   # prior weight in weighted RZ opportunities
            ov_c = rates["carry_share"] or 0
            ov_t = rates["target_share"] or 0
            rzd_rush = (pc + K * ov_c) / (tc + K) if (tc + K) else 0
            rzd_rec = (pt + K * ov_t) / (tt + K) if (tt + K) else 0
            rzd = {"n_games": int(len(rzg)), "rz_car": int(rzg.rzc20.sum()), "i10_car": int(rzg.rzc10.sum()), "i5_car": int(rzg.rzc5.sum()),
                   "rz_tgt": int(rzg.rzt20.sum()), "i10_tgt": int(rzg.rzt10.sum()), "i5_tgt": int(rzg.rzt5.sum()),
                   "rush_share": r(rzd_rush, 3), "rec_share": r(rzd_rec, 3),
                   "raw_rush_share": r(pc / tc, 3) if tc else 0.0, "raw_rec_share": r(pt / tt, 3) if tt else 0.0}
        cur_rates = {"target_share": (cur_team.targets.sum() / cur_team.team_tgt.sum()) if len(cur_team) and cur_team.team_tgt.sum() else None,
                     "carry_share": (cur_team.carries.sum() / cur_team.team_car.sum()) if len(cur_team) and cur_team.team_car.sum() else None,
                     "games": int(len(cur_team))}
        tds = {"total": int(l10.td.sum()), "games_with": int((l10.td > 0).sum()), "pg": r(l10.td.mean(), 3)}
        log = [{"season": int(x.season), "wk": int(x.week), "po": x.season_type == "POST", "opp": x.opponent_team, "team": x.team}
               for _, x in l10.iterrows()]
        # ---- H2H vs opponent, last 3 seasons
        opp = home if team == away else away
        allv = valid[valid.season >= S - 2]
        meet = allv[allv.opponent_team == opp]
        dist = g[(g.opponent_team == opp) & g.excluded & (g.season >= S - 2)]
        h2h = None
        if len(meet):
            h2h = {"n": int(len(meet)), "excluded": [f"{int(x.season)} wk{int(x.week)} (low snaps)" for _, x in dist.iterrows()],
                   "meetings": [{"season": int(x.season), "wk": int(x.week), "team": x.team, "qb": x.qb_name} for _, x in meet.iterrows()],
                   "diff_team": int((meet.team != team).sum()),
                   "diff_qb": int((meet.qb_id != cur_qb_id).sum()) if c["pos"] != "QB" else int((meet.player_id != c["id"]).sum()),
                   "metrics": {}}
            for m in METRICS[c["pos"]]:
                if m not in meet or meet[m].isna().all():
                    continue
                seas = allv[allv.season.isin(meet.season.unique())]
                savg_by = seas.groupby("season")[m].mean()
                savg = seas[m].mean()
                havg = meet[m].mean()
                above = sum(1 for _, x in meet.iterrows() if x[m] > savg_by.get(x.season, savg))
                below = sum(1 for _, x in meet.iterrows() if x[m] < savg_by.get(x.season, savg))
                h2h["metrics"][m] = {"h2h_avg": r(havg, 2), "season_avg": r(savg, 2), "delta": r(havg - savg, 2),
                                     "above": above, "below": below, "vals": [r(v, 1) for v in meet[m].tolist()]}
        return {"id": c["id"], "name": c["name"], "pos": c["pos"], "team": team, "status": c["status"],
                "l10_split": split, "flags": flags, "excluded": [f"{int(x.season)} wk{int(x.week)} vs {x.opponent_team} ({fmt(x.snap_pct,0)}% snaps)" for _, x in excl.iterrows()],
                "metrics": mets, "rates": {k: (r(v, 4) if isinstance(v, float) else v) for k, v in rates.items()},
                "rz": rzd, "tds": tds, "cur_rates": cur_rates, "log": log, "h2h": h2h}

    out_players, vac_out = [], []
    team_qb = {away: gm.get("away_qb_id"), home: gm.get("home_qb_id")}
    for t in [away, home]:
        chosen, vacated = select(t, team_qb[t])
        for c in chosen:
            out_players.append(summarize(c, t, team_qb[t]))
        for c in vacated:
            s = summarize(c, t, team_qb[t])
            cr = s["cur_rates"] if s["cur_rates"]["games"] else s["rates"]
            if (cr["target_share"] or 0) < .05 and (cr["carry_share"] or 0) < .15:
                continue
            cr = s["cur_rates"] if s["cur_rates"]["games"] else s["rates"]
            vac_out.append({"name": s["name"], "team": t, "pos": s["pos"], "status": c["status"],
                            "target_share": r(cr["target_share"], 4), "carry_share": r(cr["carry_share"], 4),
                            "rz_rush_share": s["rz"].get("rush_share", 0), "rz_rec_share": s["rz"].get("rec_share", 0)})

    # team-level H2H pass-rate pattern
    tstats = pd.concat([load("team", s) for s in seasons], ignore_index=True)
    h2h_team = {}
    if not tstats.empty:
        tstats = tstats[~((tstats.season == S) & (tstats.week >= W))]
        tstats["pr"] = (tstats.attempts + tstats.sacks_suffered) / (tstats.attempts + tstats.sacks_suffered + tstats.carries)
        for t, o in [(away, home), (home, away)]:
            mt = tstats[(tstats.team == t) & (tstats.opponent_team == o)]
            if len(mt):
                base = tstats[(tstats.team == t) & tstats.season.isin(mt.season.unique())].pr.mean()
                h2h_team[t] = {"n": int(len(mt)), "pass_rate_h2h": r(mt.pr.mean() * 100, 1), "pass_rate_season": r(base * 100, 1),
                               "higher_in": int((mt.pr > base).sum())}

    spread = gm.spread_line  # nflverse: positive = home favored by X
    total = gm.total_line
    info = {"season": S, "week": W, "away": away, "home": home, "game_id": gm.game_id, "date": gm.gameday,
            "time_et": gm.gametime, "weekday": gm.weekday, "stadium": gm.stadium, "roof": gm.roof, "surface": gm.surface,
            "div_game": bool(gm.div_game), "away_rest": r(gm.away_rest, 0), "home_rest": r(gm.home_rest, 0),
            "spread_home": r(spread, 1), "total": r(total, 1), "away_ml": r(gm.away_moneyline, 0), "home_ml": r(gm.home_moneyline, 0),
            "away_qb": gm.away_qb_name, "home_qb": gm.home_qb_name, "away_coach": gm.away_coach, "home_coach": gm.home_coach,
            "temp": r(gm.temp, 0), "wind": r(gm.wind, 0),
            "data_through": {"pbp_max_week_cur": int(pbp[pbp.season == S].week.max()) if not pbp.empty and (pbp.season == S).any() else 0,
                             "injury_week": W if inj_rows else None}}
    data = {"info": info, "teams": teams, "league": lg, "matchups": matchups, "players": out_players,
            "vacated": vac_out, "injuries": list(inj_rows.values()), "h2h_team": h2h_team, "warnings": WARN,
            "built": dt.datetime.now().strftime("%Y-%m-%d %H:%M")}
    with open(os.path.join(a.work, "data.json"), "w") as f:
        json.dump(data, f, default=lambda o: None if (isinstance(o, float) and math.isnan(o)) else str(o))
    write_summary(data, a.work)
    write_lines_template(data, a.work)
    print(open(os.path.join(a.work, "summary.txt")).read())

MARKETS = {"QB": ["pass_yds", "pass_att", "cmp", "pass_td", "int", "rush_yds", "rush_att", "long_comp"],
           "RB": ["rush_yds", "rush_att", "rec", "rec_yds", "rush_rec_yds"],
           "WR": ["rec_yds", "rec", "long_rec"], "TE": ["rec_yds", "rec", "long_rec"]}

def write_lines_template(data, work):
    rows = []
    for p in data["players"]:
        for m in MARKETS[p["pos"]]:
            rows.append({"player": p["name"], "team": p["team"], "market": m, "line": "", "odds": ""})
        rows.append({"player": p["name"], "team": p["team"], "market": "anytime_td", "line": "", "odds": ""})
    pd.DataFrame(rows).to_csv(os.path.join(work, "lines_template.csv"), index=False)

def write_summary(d, work):
    i = d["info"]
    L = []
    sh = i["spread_home"]
    fav = i["home"] if (sh or 0) > 0 else i["away"]
    L.append(f"GAME {i['away']} @ {i['home']} | {i['weekday']} {i['date']} {i['time_et']} ET | {i['stadium']} ({i['roof']}) | div={i['div_game']} | rest {i['away_rest']}/{i['home_rest']}")
    L.append(f"nflverse lines: {fav} -{abs(sh or 0)} total {i['total']} ML {i['away_ml']}/{i['home_ml']} | QBs {i['away_qb']} / {i['home_qb']} | pbp through wk {i['data_through']['pbp_max_week_cur']}")
    for t in [i["away"], i["home"]]:
        o = [x for x in d["matchups"][t]]
        L.append(f"{t} OFF vs DEF: " + "; ".join(f"{x['lane']} {x['off_rank']}v{x['def_rank']} {x['label']}" for x in o))
    for t in [i["away"], i["home"]]:
        T = d["teams"][t]
        L.append(f"{t} tend: neutral pass {fmt(T['off']['neutral_pass_rate']['v'])}% PROE {fmt(T['off']['proe']['v'])} pace {fmt(T['off'].get('pace_sec',{}).get('v'))}s plays {fmt(T['off']['plays_pg']['v'])} | def opp-neutral-pass {fmt(T['def']['neutral_pass_rate']['v'])}% (lg {fmt(d['league']['def']['neutral_pass_rate'])})")
    L.append("PLAYERS (L10 avg/med; share; RZ; H2H n):")
    for p in d["players"]:
        m = p["metrics"]
        keys = {"QB": ["pass_yds", "pass_att", "rush_yds"], "RB": ["rush_att", "rush_yds", "rec"], "WR": ["targets", "rec_yds"], "TE": ["targets", "rec_yds"]}[p["pos"]]
        ms = " ".join(f"{k} {fmt(m[k]['avg'])}/{fmt(m[k]['med'])}{'(' + m[k]['trend'][0] + ')'}" for k in keys if m.get(k))
        rt = p["rates"]
        sh = f"tgt {fmt((rt['target_share'] or 0)*100,0)}% car {fmt((rt['carry_share'] or 0)*100,0)}% snap {fmt(rt['snap_pct'],0)}%"
        rz = p["rz"]
        rzs = f"rzC {rz.get('rz_car',0)}/{rz.get('i10_car',0)} rzT {rz.get('rz_tgt',0)}/{rz.get('i10_tgt',0)}" if rz else "rz N/A"
        L.append(f" {p['team']} {p['pos']} {p['name']} [{p['status']}] {p['l10_split']} {'FLAG:' + ','.join(p['flags']) if p['flags'] else ''} | {ms} | {sh} | {rzs} | TD {p['tds']['total']} ({p['tds']['games_with']}/10) | H2H n={p['h2h']['n'] if p['h2h'] else 0}")
    if d["vacated"]:
        L.append("VACATED (Out/Doubtful would-be in-scope): " + "; ".join(f"{v['team']} {v['name']} {v['status']} tgt {fmt((v['target_share'] or 0)*100,0)}% car {fmt((v['carry_share'] or 0)*100,0)}%" for v in d["vacated"]))
    L.append("INJURY REPORT: " + "; ".join(f"{x['team']} {x['pos']} {x['name']} {x['status']} ({x['practice'][:20]})" for x in d["injuries"]))
    if d["h2h_team"]:
        L.append("H2H team pass rate: " + "; ".join(f"{t} {v['pass_rate_h2h']}% vs season {v['pass_rate_season']}% (higher in {v['higher_in']}/{v['n']})" for t, v in d["h2h_team"].items()))
    if d["warnings"]:
        L.append("WARNINGS: " + " | ".join(d["warnings"]))
    with open(os.path.join(work, "summary.txt"), "w") as f:
        f.write("\n".join(L) + "\n")

# ----------------------------------------------------------------------------- lines via The Odds API (optional)
TEAM_NAMES = {"ARI": "Arizona Cardinals", "ATL": "Atlanta Falcons", "BAL": "Baltimore Ravens", "BUF": "Buffalo Bills",
    "CAR": "Carolina Panthers", "CHI": "Chicago Bears", "CIN": "Cincinnati Bengals", "CLE": "Cleveland Browns",
    "DAL": "Dallas Cowboys", "DEN": "Denver Broncos", "DET": "Detroit Lions", "GB": "Green Bay Packers",
    "HOU": "Houston Texans", "IND": "Indianapolis Colts", "JAX": "Jacksonville Jaguars", "KC": "Kansas City Chiefs",
    "LV": "Las Vegas Raiders", "LAC": "Los Angeles Chargers", "LA": "Los Angeles Rams", "MIA": "Miami Dolphins",
    "MIN": "Minnesota Vikings", "NE": "New England Patriots", "NO": "New Orleans Saints", "NYG": "New York Giants",
    "NYJ": "New York Jets", "PHI": "Philadelphia Eagles", "PIT": "Pittsburgh Steelers", "SF": "San Francisco 49ers",
    "SEA": "Seattle Seahawks", "TB": "Tampa Bay Buccaneers", "TEN": "Tennessee Titans", "WAS": "Washington Commanders"}
ODDS_MARKETS = {"player_pass_yds": "pass_yds", "player_pass_attempts": "pass_att", "player_pass_completions": "cmp",
    "player_pass_tds": "pass_td", "player_pass_interceptions": "int", "player_rush_attempts": "rush_att",
    "player_rush_yds": "rush_yds", "player_receptions": "rec", "player_reception_yds": "rec_yds",
    "player_reception_longest": "long_rec", "player_pass_longest_completion": "long_comp",
    "player_rush_reception_yds": "rush_rec_yds", "player_anytime_td": "anytime_td", "player_1st_td": "first_td"}

def cmd_lines(a):
    d = json.load(open(os.path.join(a.work, "data.json")))
    i = d["info"]
    base = "https://api.the-odds-api.com/v4/sports/americanfootball_nfl"
    def get(url):
        rr = subprocess.run(["curl", "-sf", "--max-time", "60", url], capture_output=True, text=True)
        if rr.returncode != 0:
            print("STOP: The Odds API unreachable. Add api.the-odds-api.com to allowed domains in Settings, or paste lines.")
            sys.exit(3)
        return json.loads(rr.stdout)
    evs = get(f"{base}/events?apiKey={a.api_key}")
    ev = next((e for e in evs if e["home_team"] == TEAM_NAMES[i["home"]] and e["away_team"] == TEAM_NAMES[i["away"]]), None)
    if not ev:
        print("STOP: event not found in The Odds API feed."); sys.exit(3)
    mk = ",".join(ODDS_MARKETS)
    od = get(f"{base}/events/{ev['id']}/odds?apiKey={a.api_key}&regions=us&markets={mk}&oddsFormat=american&bookmakers={a.book}")
    rows = []
    for bk in od.get("bookmakers", []):
        for m in bk.get("markets", []):
            key = ODDS_MARKETS.get(m["key"])
            for o in m.get("outcomes", []):
                if o.get("name") in ("Over", "Yes") or key in ("anytime_td", "first_td"):
                    if o.get("name") in ("Under", "No"):
                        continue
                    rows.append({"player": o.get("description", ""), "team": "", "market": key,
                                 "line": o.get("point", ""), "odds": o.get("price", "")})
    out = os.path.join(a.work, "lines.csv")
    pd.DataFrame(rows).to_csv(out, index=False)
    print(f"wrote {len(rows)} lines from {a.book} to {out}")

# ----------------------------------------------------------------------------- report command
YARD_MKTS = {"pass_yds", "rush_yds", "rec_yds", "rush_rec_yds", "long_rec", "long_comp"}
MKT_LABEL = {"pass_yds": "Pass Yds", "pass_att": "Pass Att", "cmp": "Completions", "pass_td": "Pass TDs", "int": "INTs",
             "rush_yds": "Rush Yds", "rush_att": "Rush Att", "rec": "Receptions", "rec_yds": "Rec Yds",
             "rush_rec_yds": "Rush+Rec Yds", "long_rec": "Longest Rec", "long_comp": "Longest Comp",
             "targets": "Targets", "td": "TDs"}
H2H_METRICS = {"pass_yds", "pass_att", "cmp", "rush_att", "rush_yds", "targets", "rec", "rec_yds", "rush_rec_yds"}

def american_to_prob(o):
    try:
        o = float(o)
    except Exception:
        return None
    return 100 / (o + 100) if o > 0 else abs(o) / (abs(o) + 100)

def prob_over(mu, line, std=None):
    """P(X > line) for count stats. Poisson under 10; normal with sd=max(sqrt(mu), L10 sd) above."""
    if mu is None or mu <= 0:
        return 0.0
    k = math.floor(line)            # over a .5 line means X >= k+1
    if mu < 10:
        cdf = sum(math.exp(-mu) * mu ** j / math.factorial(j) for j in range(0, k + 1))
        return 1 - cdf
    sd = max(math.sqrt(mu), std or 0)
    return 1 - norm_cdf((k + 0.5 - mu) / sd)

def cmd_report(a):
    d = json.load(open(os.path.join(a.work, "data.json")))
    ctx = json.load(open(a.context)) if a.context and os.path.exists(a.context) else {}
    lines = pd.read_csv(a.lines, dtype=str).fillna("") if a.lines and os.path.exists(a.lines) else pd.DataFrame(columns=["player", "market", "line", "odds"])
    i = d["info"]
    A, H = i["away"], i["home"]
    spread_home = ctx.get("spread_home", i["spread_home"]) or 0.0   # + = home favored
    total = ctx.get("total", i["total"]) or 44.0
    fav, dog = (H, A) if spread_home > 0 else (A, H)
    sp_abs = abs(spread_home)
    implied = {fav: (total + sp_abs) / 2, dog: (total - sp_abs) / 2}
    team_spread = {fav: -sp_abs, dog: sp_abs}   # negative = favored
    opp_of = {A: H, H: A}

    # statuses (context overrides nflverse)
    status = {x["name"]: {"status": x["status"], "practice": x["practice"], "injury": x.get("injury", ""), "team": x["team"], "pos": x["pos"]} for x in d["injuries"]}
    for nm, v in ctx.get("status", {}).items():
        status.setdefault(nm, {"team": v.get("team", ""), "pos": v.get("pos", ""), "injury": ""}).update(v)
    for p in d["players"]:
        if p["name"] in status:
            p["status"] = status[p["name"]].get("status", p["status"])
    active = [p for p in d["players"] if p["status"] not in ("Out", "Doubtful")]
    dropped = [p for p in d["players"] if p["status"] in ("Out", "Doubtful")]
    vac = d["vacated"] + [{"name": p["name"], "team": p["team"], "pos": p["pos"], "status": p["status"],
                           "target_share": p["rates"]["target_share"], "carry_share": p["rates"]["carry_share"],
                           "rz_rush_share": p["rz"].get("rush_share", 0), "rz_rec_share": p["rz"].get("rec_share", 0)} for p in dropped]

    # labels (with overrides)
    labels = {}
    for t in [A, H]:
        for x in d["matchups"][t]:
            labels[(t, x["lane"])] = ctx.get("label_override", {}).get(f"{t}:{x['lane']}", x["label"])
    def lab(t, lane):
        return labels.get((t, lane), "N/A")

    # ---------- team volume (04 step 5)
    vol = {}
    for t in [A, H]:
        o, dd = d["teams"][t]["off"], d["teams"][opp_of[t]]["def"]
        plays = np.nanmean([o["plays_pg"]["v"] or np.nan, dd["plays_pg"]["v"] or np.nan])
        plays += ctx.get("plays_adj", {}).get(t, 0)
        dbr = (o["dropback_rate"]["v"] or 58) / 100
        dbr += -0.006 * (-team_spread[t]) if True else 0     # favorite passes less, dog more
        dbr += ctx.get("pass_rate_adj", {}).get(t, 0)
        sack = np.nanmean([o["sack_rate"]["v"] or np.nan, dd["sack_rate"]["v"] or np.nan]) / 100
        scr = (o["scramble_rate"]["v"] or 3) / 100
        dbs = plays * dbr
        pass_att = dbs * (1 - sack - scr)
        rush_att = plays - dbs + dbs * scr
        vol[t] = {"plays": plays, "db_rate": dbr, "pass_att": pass_att, "rush_att": rush_att, "sack": sack}

    # ---------- share redistribution
    so = ctx.get("share_override", {})
    for t in [A, H]:
        tv = [v for v in vac if v["team"] == t]
        vt = sum((v["target_share"] or 0) for v in tv) * 0.8
        n_rb = len([p for p in active if p["team"] == t and p["pos"] == "RB"])
        rb_keep = 0.9 if n_rb >= 2 else 0.5   # lone in-scope RB does not absorb everything (unscoped backup gets some)
        vc = sum((v["carry_share"] or 0) for v in tv if v["pos"] == "RB") * rb_keep
        vrr = sum((v.get("rz_rush_share") or 0) for v in tv if v["pos"] == "RB") * rb_keep
        vrt = sum((v.get("rz_rec_share") or 0) for v in tv) * 0.8
        tp = [p for p in active if p["team"] == t]
        catchers = [p for p in tp if p["pos"] != "QB"]
        rbs = [p for p in tp if p["pos"] == "RB"]
        st = sum(p["rates"]["target_share"] or 0 for p in catchers) or 1
        sc = sum(p["rates"]["carry_share"] or 0 for p in rbs) or 1
        srr = sum(p["rz"].get("rush_share", 0) for p in rbs) or 1
        srt = sum(p["rz"].get("rec_share", 0) for p in catchers) or 1
        for p in tp:
            ts = p["rates"]["target_share"] or 0
            cs = p["rates"]["carry_share"] or 0
            p["proj_ts"] = ts + (vt * ts / st if p in catchers else 0)
            p["proj_cs"] = cs + (vc * cs / sc if p in rbs else 0)
            rr, rt = p["rz"].get("rush_share", 0), p["rz"].get("rec_share", 0)
            p["proj_rzr"] = rr + (vrr * rr / srr if p in rbs else 0)
            p["proj_rzt"] = rt + (vrt * rt / srt if p in catchers else 0)
            ov = so.get(p["name"], {})
            p["proj_ts"] = ov.get("target_share", p["proj_ts"]); p["proj_cs"] = ov.get("carry_share", p["proj_cs"])
            p["proj_rzr"] = ov.get("rz_rush_share", p["proj_rzr"]); p["proj_rzt"] = ov.get("rz_rec_share", p["proj_rzt"])
            p["vac_bump"] = bool((vt and p in catchers) or (vc and p in rbs))

    # ---------- H2H adjustment helper (05)
    dc_changed = ctx.get("dc_changed", {})
    def h2h_adj(p, m, base):
        h = p.get("h2h")
        if not h or m not in h["metrics"] or m not in H2H_METRICS:
            return 0.0, None
        x = h["metrics"][m]
        n = h["n"]
        side = max(x["above"], x["below"])
        agree = (x["delta"] > 0 and x["above"] >= x["below"]) or (x["delta"] < 0 and x["below"] >= x["above"])
        if n < 3 or side / n < 0.75 or not agree:
            return 0.0, {"qual": False, **x, "n": n, "weight": 0}
        wgt = 0.25 if n == 3 else (0.40 if n <= 5 else 0.50)
        if h["diff_qb"] > n / 2 or h["diff_team"] > n / 2 or dc_changed.get(opp_of[p["team"]]):
            wgt /= 2
        adj = wgt * x["delta"]
        cap = 0.15 * base
        adj = max(-cap, min(cap, adj))
        return adj, {"qual": True, **x, "n": n, "weight": wgt, "adj": adj}

    # ---------- projections
    def expl_lab(t):
        return lab(t, "Explosives/depth")
    for p in active:
        t, pos, rt, M = p["team"], p["pos"], p["rates"], p["metrics"]
        v = vol[t]
        proj, meta = {}, {}
        if pos == "QB":
            l1 = LABEL_ADJ[lab(t, "Pass efficiency")]
            att = v["pass_att"]
            proj["pass_att"] = att
            proj["cmp"] = att * (rt["comp_pct"] or .64) * (1 + l1 / 2)
            proj["pass_yds"] = att * (rt["yds_per_att"] or 6.8) * (1 + l1)
            proj["pass_td"] = att * (rt["td_rate"] or .045) * (1 + l1)
            proj["int"] = att * (rt["int_rate"] or .022) * (1 - l1 / 2)
            proj["rush_att"] = v["rush_att"] * (rt["carry_share"] or 0)
            proj["rush_yds"] = proj["rush_att"] * (rt["ypc"] or 0)
            if M.get("long_comp"):
                proj["long_comp"] = M["long_comp"]["med"] * (1 + LABEL_ADJ[expl_lab(t)])
            meta["vol"] = {"pass_att": proj["pass_att"], "pass_yds": proj["pass_att"], "cmp": proj["pass_att"], "pass_td": proj["pass_att"],
                           "int": proj["pass_att"], "rush_att": proj["rush_att"], "rush_yds": proj["rush_att"], "long_comp": proj["pass_att"]}
            meta["vol_base"] = {k: (M["pass_att"]["wavg"] if k not in ("rush_att", "rush_yds") else (M["rush_att"] or {}).get("wavg")) for k in meta["vol"]}
            meta["lane"] = {k: "Pass efficiency" for k in meta["vol"]}
            meta["lane"].update({"rush_att": "Run game", "rush_yds": "Run game", "long_comp": "Explosives/depth"})
        else:
            lane = {"WR": "WR targets", "TE": "TE targets", "RB": "RB receiving"}[pos]
            la = LABEL_ADJ_RECV[lab(t, lane)]
            # target share is of TEAM TARGETS, which run ~5-8% below pass attempts (throwaways, spikes)
            tg = v["pass_att"] * min(1.0, rt.get("tgt_per_att") or 0.93) * p["proj_ts"]
            proj["targets"] = tg
            proj["rec"] = tg * (rt["catch_rate"] or .65) * (1 + la / 2)
            proj["rec_yds"] = tg * (rt["yds_per_tgt"] or 7) * (1 + la)
            if pos in ("WR", "TE") and M.get("long_rec"):
                proj["long_rec"] = M["long_rec"]["med"] * (1 + LABEL_ADJ[expl_lab(t)])
            meta["lane"] = {"targets": lane, "rec": lane, "rec_yds": lane, "long_rec": "Explosives/depth"}
            meta["vol"] = {"targets": tg, "rec": tg, "rec_yds": tg, "long_rec": tg}
            tb = (M.get("targets") or {}).get("wavg")
            meta["vol_base"] = {"targets": tb, "rec": tb, "rec_yds": tb, "long_rec": tb}
            if pos == "RB":
                lr = LABEL_ADJ[lab(t, "Run game")]
                ra = v["rush_att"] * p["proj_cs"]
                proj["rush_att"] = ra
                proj["rush_yds"] = ra * (rt["ypc"] or 4.2) * (1 + lr)
                proj["rush_rec_yds"] = proj["rush_yds"] + proj["rec_yds"]
                cb = (M.get("rush_att") or {}).get("wavg")
                meta["lane"].update({"rush_att": "Run game", "rush_yds": "Run game", "rush_rec_yds": "Run game"})
                meta["vol"].update({"rush_att": ra, "rush_yds": ra, "rush_rec_yds": ra + tg})
                meta["vol_base"].update({"rush_att": cb, "rush_yds": cb, "rush_rec_yds": (cb or 0) + (tb or 0)})
        # H2H
        h2 = {}
        for m in list(proj):
            adj, info = h2h_adj(p, m, proj[m])
            if info:
                h2[m] = info
            if adj:
                meta.setdefault("base", {})[m] = proj[m]
                proj[m] += adj
        p["proj"], p["meta"], p["h2h_used"] = proj, meta, h2

    # ---------- TD model (07)
    td_team = {}
    for t in [A, H]:
        o, dd = d["teams"][t]["off"], d["teams"][opp_of[t]]["def"]
        fg = o["fg_pg"]["v"] or 1.7
        exp_td = max(0.5, (implied[t] - 3 * fg) / 7)
        rtd, ptd = o["rush_td_pg"]["v"] or 0.9, o["pass_td_pg"]["v"] or 1.4
        rs = rtd / (rtd + ptd) if (rtd + ptd) else 0.4
        notes = []
        rl = lab(t, "Run game")
        if rl in ("ATTACK", "LEAN ATTACK"):
            rs += .05 if rl == "ATTACK" else .025; notes.append(f"run lane {rl}")
        pl = lab(t, "Pass efficiency")
        if pl in ("ATTACK", "LEAN ATTACK"):
            rs -= .05 if pl == "ATTACK" else .025; notes.append(f"pass lane {pl}")
        if t == fav and sp_abs >= 3:
            rs += .03; notes.append("favorite lean rush")
        elif t == dog and sp_abs >= 3:
            rs -= .03; notes.append("underdog lean pass")
        dr, dp = dd["rush_td_pg"]["v"] or 0, dd["pass_td_pg"]["v"] or 0
        lgr = d["league"]["def"]["rush_td_pg"] / (d["league"]["def"]["rush_td_pg"] + d["league"]["def"]["pass_td_pg"])
        if dr + dp and dr / (dr + dp) > lgr + .10:
            rs += .03; notes.append("opp allows high rush-TD share")
        orz, drz = o["rz_td_pct"]["rank"], dd["rz_td_pct"]["rank"]
        if orz and drz and orz <= 8 and drz >= 25:
            exp_td *= 1.05; notes.append("RZ offense strong vs weak RZ D (+5%)")
        elif orz and drz and orz >= 25 and drz <= 8:
            exp_td *= 0.95; notes.append("RZ offense weak vs strong RZ D (-5%)")
        rs = min(.75, max(.2, rs))
        td_team[t] = {"exp": exp_td, "rush": exp_td * rs, "pass": exp_td * (1 - rs), "fg": fg, "notes": notes}
    odds = {}
    for _, x in lines.iterrows():
        if x.market in ("anytime_td", "first_td") and str(x.odds).strip():
            odds[(norm_name(x.player), x.market)] = x.odds
    td_rows = []
    for t in [A, H]:
        tp = [p for p in active if p["team"] == t]
        rbs = sorted([p for p in tp if p["pos"] == "RB"], key=lambda p: -p["proj_rzr"])
        committee = bool(rbs) and rbs[0]["proj_rzr"] < 0.5 and len(rbs) > 1 and rbs[1]["proj_rzr"] >= 0.3
        for p in tp:
            e = td_team[t]["rush"] * p["proj_rzr"] + (0 if p["pos"] == "QB" else td_team[t]["pass"] * p["proj_rzt"])
            note = ""
            h = p.get("h2h")
            if h and h["n"] >= 3 and "td" in h["metrics"]:
                dlt = h["metrics"]["td"]["delta"]
                if abs(dlt) >= 0.25:
                    e *= 1.10 if dlt > 0 else 0.90; note = f"H2H nudge {'+' if dlt > 0 else '-'}10%"
            prob = 1 - math.exp(-e)
            o_any = odds.get((norm_name(p["name"]), "anytime_td"))
            bk = american_to_prob(o_any) if o_any else None
            edge = (prob - bk) * 100 if bk is not None else None
            qst = p["status"] == "Questionable"
            if edge is None:
                label = "No odds"
            elif prob >= .40 and edge >= 4 and not qst and not (committee and p["pos"] == "RB"):
                label = "Core"
            elif edge >= 6 and prob >= .20:
                label = "Value"
            elif edge >= 6 and prob < .20:
                label = "Longshot"
            else:
                label = "Avoid"
            l10pg = p["tds"]["pg"] or 0
            verdict = "AT" if abs(e - l10pg) <= 0.15 else ("ABOVE" if e > l10pg else "BELOW")
            fo = odds.get((norm_name(p["name"]), "first_td"))
            first = None
            if fo:
                pt = implied[t] / (implied[A] + implied[H])
                pf = pt * e / td_team[t]["exp"]
                first = {"model": pf, "book": american_to_prob(fo), "odds": fo}
            td_rows.append({"p": p, "exp": e, "prob": prob, "odds": o_any, "book": bk, "edge": edge, "label": label,
                            "verdict": verdict, "note": note, "committee": committee and p["pos"] == "RB", "first": first})
            p["proj"]["td"] = e

    # ---------- verdicts for all metrics
    def verdict(proj, avg):
        if proj is None or avg is None:
            return "N/A"
        if avg < 5:
            return "AT" if abs(proj - avg) < 0.5 else ("ABOVE" if proj > avg else "BELOW")
        return "AT" if abs(proj - avg) < 0.10 * avg else ("ABOVE" if proj > avg else "BELOW")

    # ---------- spotlight (05 part 4)
    groups = {}
    sflags = ctx.get("spotlight_flags", {})
    for t in [A, H]:
        gdef = {"QB passing": "Pass efficiency", "WR": "WR targets", "TE": "TE targets", "RB rushing": "Run game", "RB receiving": "RB receiving"}
        tv = [v for v in vac if v["team"] == t]
        for gname, lane in gdef.items():
            crit = []
            need = () if lane in RECV_LANES else ("ATTACK", "LEAN ATTACK")
            if lab(t, lane) in need:
                crit.append(f"{lane} {lab(t, lane)}")
            # data criterion: group's lead player volume trending up (last-3 vs L10)
            vm = {"QB passing": ("QB", "pass_att"), "WR": ("WR", "targets"), "TE": ("TE", "targets"),
                  "RB rushing": ("RB", "rush_att"), "RB receiving": ("RB", "targets")}[gname]
            lead = [p for p in active if p["team"] == t and p["pos"] == vm[0]]
            if lead and (lead[0]["metrics"].get(vm[1]) or {}).get("trend") == "Up":
                crit.append(f"{lead[0]['name']} {vm[1]} trending up (last 3 vs L10)")
            for v in tv:
                if v["pos"] == "WR" and (v["target_share"] or 0) >= .08 and gname in ("WR", "TE"):
                    crit.append(f"vacated targets ({v['name']})")
                if v["pos"] == "TE" and (v["target_share"] or 0) >= .08 and gname in ("WR", "RB receiving"):
                    crit.append(f"vacated targets ({v['name']})")
                if v["pos"] == "RB" and (v["carry_share"] or 0) >= .20 and gname == "RB rushing":
                    crit.append(f"vacated carries ({v['name']})")
            analyst = [c for c in sflags.get(f"{t}:{gname}", [])]
            if analyst:   # narrative criteria (scheme/coaching/script) count as ONE criterion at most
                crit.append("analyst: " + " / ".join(analyst))
            crit = list(dict.fromkeys(crit))
            groups[(t, gname)] = crit
    spot = {k for k, v in groups.items() if len(v) >= 2}
    def group_of(p, m):
        if p["pos"] == "QB":
            return "QB passing" if m not in ("rush_att", "rush_yds") else None
        if p["pos"] == "RB":
            return "RB rushing" if m in ("rush_att", "rush_yds", "rush_rec_yds") else "RB receiving"
        return p["pos"]

    # ---------- prop scoring (05 part 5)
    robust_over = ctx.get("robust", {})
    returning = set(ctx.get("returning", []))
    role_change = set(ctx.get("role_change", []))
    wx_unc = ctx.get("weather_uncertain", False)
    byname = {norm_name(p["name"]): p for p in active}
    key_mates = {t: [p for p in active if p["team"] == t and p["pos"] == "QB"] for t in [A, H]}
    props, unmatched = [], []
    for _, x in lines.iterrows():
        m = x.market
        if m in ("anytime_td", "first_td") or not str(x.line).strip():
            continue
        p = byname.get(norm_name(x.player))
        if not p or m not in p["proj"]:
            unmatched.append(f"{x.player} {m}")
            continue
        line = float(x.line)
        proj = p["proj"][m]
        Mm = p["metrics"].get(m) or {}
        avg, med = Mm.get("avg"), Mm.get("med")
        games = Mm.get("games") or []
        hits_o = sum(1 for g in games if g is not None and g > line)
        hits_u = sum(1 for g in games if g is not None and g < line)
        edge = proj - line
        edge_pct = edge / line if line else 0
        yards = m in YARD_MKTS
        p_over = None
        if yards:
            # Yardage is right-skewed: center on the projected MEDIAN (proj x L10 median/avg) and use the
            # player's own spread. A flat % threshold let low-volume players (TE, WR3) clear it on 2-3 yards of noise.
            skew = (med / avg) if (med and avg) else 0.9
            center = proj * min(1.0, max(0.6, skew))
            sd = max(Mm.get("std") or 0, 0.45 * max(proj, 1))
            p_over = 1 - norm_cdf((line - center) / sd)
            imp = american_to_prob(x.odds) if str(x.odds).strip() else None
            thr_o = max(.57, (imp or 0) + .03)
            lean = "Over" if p_over >= thr_o and edge_pct >= .08 else ("Under" if (1 - p_over) >= .57 and edge_pct <= -.08 else "Pass")
        else:
            p_over = prob_over(proj, line, Mm.get("std"))
            imp = american_to_prob(x.odds) if str(x.odds).strip() else None
            thr_o = max(.57, (imp or 0) + .03)
            lean = "Over" if p_over >= thr_o else ("Under" if (1 - p_over) >= .57 else "Pass")
        sgn = 1 if lean == "Over" else -1
        grp = group_of(p, m)
        in_spot = (p["team"], grp) in spot if grp else False
        # usage 0-8
        vb, vp = p["meta"]["vol_base"].get(m), p["meta"]["vol"].get(m)
        vd = (vp / vb - 1) * sgn if vb else 0
        u_vol = max(0, min(4, 2 + 20 * vd))
        cv = (Mm.get("std") or 0) / avg if avg else 1
        vol_metric = {"QB": "pass_att", "RB": "rush_att" if m in ("rush_att", "rush_yds", "rush_rec_yds") else "targets"}.get(p["pos"], "targets")
        vm = p["metrics"].get(vol_metric) or {}
        vcv = (vm.get("std") or 0) / vm["avg"] if vm.get("avg") else 1
        u_stab = 2 if vcv < .30 else (1 if vcv < .50 else 0)
        usage = min(8, u_vol + u_stab + (2 if in_spot else 0))
        # matchup 0-5
        L = lab(p["team"], p["meta"]["lane"].get(m, "Pass efficiency"))
        over_pts = {"ATTACK": 4, "LEAN ATTACK": 3, "NEUTRAL": 2, "VOLATILE": 1, "CONTESTED": 1, "LEAN FADE": 1, "FADE": 0, "N/A": 1}
        under_pts = {"FADE": 4, "LEAN FADE": 3, "CONTESTED": 2, "NEUTRAL": 2, "VOLATILE": 1, "LEAN ATTACK": 1, "ATTACK": 0, "N/A": 1}
        if lean != "Under" and p["meta"]["lane"].get(m) in RECV_LANES:
            over_pts = dict(over_pts, **{"ATTACK": 2, "LEAN ATTACK": 2})
        match = (over_pts if lean != "Under" else under_pts)[L]
        hu = p["h2h_used"].get(m)
        h2h_conf = False
        if hu and hu.get("qual"):
            if (hu["delta"] > 0) == (lean == "Over"):
                match = min(5, match + 1)
            elif lean != "Pass":
                h2h_conf = True
        # script robustness 0-4
        isfav = p["team"] == fav
        if m in ("rec", "targets", "cmp"):
            rob = 3
        elif m in ("rec_yds", "rush_rec_yds"):
            rob = 2
        elif m in ("long_rec", "long_comp", "pass_td", "int"):
            rob = 1
        elif m in ("pass_yds", "pass_att"):
            rob = (2 if isfav else 3) if lean == "Over" else (3 if isfav else 1)
        else:  # rush
            rob = (3 if isfav else 1) if lean == "Over" else (1 if isfav else 3)
        rob = robust_over.get(f"{p['name']}|{m}", rob)
        # line value 0-4
        pl_ = p_over if lean != "Under" else 1 - p_over
        lv = 4 if pl_ >= .68 else 3 if pl_ >= .63 else 2 if pl_ >= .57 else 0
        # consistency 0-2
        hr = hits_o if lean == "Over" else hits_u
        n = len(games) or 10
        cons = 2 if hr / n >= .7 else (1 if hr / n >= .6 else 0)
        # personnel 0-2
        mate_q = any(q["status"] == "Questionable" for q in key_mates[p["team"]] if q is not p)
        pers = 0 if p["status"] == "Questionable" else (1 if mate_q else 2)
        score = usage + match + rob + lv + cons + pers - (1 if h2h_conf else 0)
        score = round(score)
        cap, capnote = 25, []
        if p["status"] == "Questionable" or mate_q:
            cap = min(cap, 20); capnote.append("Questionable tag")
        if p["name"] in returning:
            cap = min(cap, 16); capnote.append("returning/snap limit")
        na_ct = sum(1 for k in ("target_share", "carry_share", "snap_pct") if p["rates"].get(k) is None)
        if na_ct >= 2 and p["pos"] != "QB":
            cap = min(cap, 16); capnote.append("2+ key stats N/A")
        if len(games) < 5:
            cap = min(cap, 16); capnote.append(f"small sample (L{len(games)})")
        if wx_unc and m in ("pass_yds", "pass_att", "cmp", "rec_yds", "rec", "long_rec", "long_comp"):
            cap = min(cap, 20); capnote.append("weather uncertain")
        if (p["flags"] or p["name"] in role_change) and sum(1 for g in p["log"] if g["season"] == i["season"]) < 4:
            cap = min(cap, 20); capnote.append("early-season change (" + ",".join(p["flags"] or ["role"]) + ")")
        if hu and hu.get("qual") and lean != "Pass":
            base = p["meta"].get("base", {}).get(m, proj)
            be = base - line
            if (yards and abs(be / line) < .08) or (not yards and abs(be) < .5):
                cap = min(cap, 16); capnote.append("edge depends on H2H")
        score = min(score, cap)
        tier = 1 if score >= 21 else 2 if score >= 17 else 3 if score >= 13 else 0
        if lean == "Pass":
            tier = 0
        props.append({"p": p, "m": m, "line": line, "odds": x.odds, "proj": proj, "avg": avg, "med": med,
                      "hit": f"{hits_o}/{len(games)}", "hit_u": f"{hits_u}/{len(games)}", "edge": edge, "edge_pct": edge_pct,
                      "lean": lean, "score": score, "p_over": p_over, "tier": tier, "spot": in_spot, "vs": verdict(proj, avg),
                      "parts": dict(usage=round(usage, 1), match=match, rob=rob, lv=lv, cons=cons, pers=pers),
                      "caps": capnote, "h2h": hu, "h2h_conf": h2h_conf, "label": L, "grp": grp})
    props.sort(key=lambda z: (-(z["tier"] > 0), z["tier"] if z["tier"] else 9, -z["spot"], -z["score"], -abs(z["edge_pct"])))
    ranked = [z for z in props if z["tier"] > 0][:8]
    fades = [z for z in props if z["lean"] == "Under" and z["tier"] > 0]

    td_rows.sort(key=lambda z: ({"Core": 0, "Value": 1, "Longshot": 2, "Avoid": 3, "No odds": 4}[z["label"]], -(z["edge"] or -99), -z["prob"]))
    td_picks = [z for z in td_rows if z["label"] in ("Core", "Value", "Longshot")][:6]
    td_avoid = sorted([z for z in td_rows if z["label"] == "Avoid"], key=lambda z: -(z["book"] or 0))[:4]

    build_pdf(a, d, ctx, dict(implied=implied, fav=fav, dog=dog, total=total, sp_abs=sp_abs, vol=vol, labels=labels, lab=lab,
                              td_team=td_team, td_rows=td_rows, td_picks=td_picks, td_avoid=td_avoid, props=props, ranked=ranked,
                              fades=fades, groups=groups, spot=spot, active=active, vac=vac, status=status, verdict=verdict,
                              unmatched=unmatched, lines=lines))

# ----------------------------------------------------------------------------- scenarios / text defaults
def norm_cdf(x):
    return 0.5 * (1 + math.erf(x / math.sqrt(2)))

def default_scenarios(d, R):
    fav, dog, sp, tot = R["fav"], R["dog"], R["sp_abs"], R["total"]
    A, H = d["info"]["away"], d["info"]["home"]
    p_fav = 1 - norm_cdf((7 - sp) / 13.5)
    p_dog = norm_cdf((-7 - sp) / 13.5)
    rest = 1 - p_fav - p_dog
    alt = "Shootout" if tot >= 46 else "Slugfest"
    base, altp = rest * .7, rest * .3
    im = R["implied"]
    pl = {t: R["vol"][t]["plays"] for t in (A, H)}
    def plays(da, dh):
        return f"{pl[A] + da:.0f} / {pl[H] + dh:.0f}"
    rows = [["Base case", f"{base*100:.0f}%", f"{fav} {im[fav]:.0f}-{dog} {im[dog]:.0f} (+/-7)", plays(0, 0), "Neutral",
             "Volume-driven props, TE/RB receptions", "Script-dependent RB carries"],
            [f"{fav} controls", f"{p_fav*100:.0f}%", f"{fav} by 8+", plays(-3 if fav == H else 3, 3 if fav == H else -3),
             f"{fav} run, {dog} pass", f"{fav} RB carries, {dog} QB att/WR volume", f"{fav} QB att, {dog} RB carries"],
            [f"{dog} controls", f"{p_dog*100:.0f}%", f"{dog} by 8+", plays(3 if fav == H else -3, -3 if fav == H else 3),
             f"{dog} run, {fav} pass", f"{fav} QB att/WR volume, {dog} RB carries", f"{fav} RB carries"],
            [alt, f"{altp*100:.0f}%", f"Total {'over ' + str(tot + 7) if alt == 'Shootout' else 'under ' + str(tot - 7)}", plays(2, 2) if alt == "Shootout" else plays(-3, -3),
             "Pass-heavy both" if alt == "Shootout" else "Run-heavy both", "QB/WR yardage overs" if alt == "Shootout" else "Yardage unders, short receptions",
             "Unders" if alt == "Shootout" else "WR/QB yardage overs"]]
    return rows

# ----------------------------------------------------------------------------- PDF
def build_pdf(a, d, ctx, R):
    from reportlab.lib.pagesizes import letter, landscape
    from reportlab.lib import colors
    from reportlab.lib.units import inch
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak, KeepTogether
    from reportlab.pdfbase import pdfmetrics
    from reportlab.pdfbase.ttfonts import TTFont
    font, bold = "Helvetica", "Helvetica-Bold"
    dv = "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"
    if os.path.exists(dv) and os.path.exists(dv.replace(".ttf", "-Bold.ttf")):
        pdfmetrics.registerFont(TTFont("DV", dv)); pdfmetrics.registerFont(TTFont("DVB", dv.replace(".ttf", "-Bold.ttf")))
        from reportlab.pdfbase.pdfmetrics import registerFontFamily
        registerFontFamily("DV", normal="DV", bold="DVB", italic="DV", boldItalic="DVB")
        font, bold = "DV", "DVB"
    i = d["info"]; A, H = i["away"], i["home"]
    ss = getSampleStyleSheet()
    body = ParagraphStyle("b", parent=ss["Normal"], fontName=font, fontSize=10, leading=13)
    small = ParagraphStyle("s", parent=body, fontSize=8, leading=10)
    cell9 = ParagraphStyle("c9", parent=body, fontSize=8.5, leading=10.5)
    h1 = ParagraphStyle("h1", parent=ss["Title"], fontName=bold, fontSize=17, spaceAfter=4, alignment=0)
    h2 = ParagraphStyle("h2", parent=ss["Heading2"], fontName=bold, fontSize=13, spaceBefore=10, spaceAfter=4)
    h3 = ParagraphStyle("h3", parent=ss["Heading3"], fontName=bold, fontSize=11, spaceBefore=6, spaceAfter=3)
    FILL = {"1": "#c8e6c9", "Core": "#c8e6c9", "2": "#bbdefb", "Value": "#bbdefb", "3": "#e0e0e0", "Longshot": "#e0e0e0",
            "Avoid": "#ffcdd2", "Fade": "#ffcdd2", "Under": None}
    def esc(s):
        return str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("\u2212", "-")
    def P(s, st=body):
        return Paragraph(esc(s), st)
    def T(rows, widths, fs=8.5, color_col=None):
        st = small if fs <= 8 else cell9
        data = [[Paragraph("<b>" + esc(c) + "</b>", st) for c in rows[0]]] + [[Paragraph(esc(c), st) for c in rr] for rr in rows[1:]]
        tot = sum(widths); widths = [w * 720 / tot for w in widths]
        t = Table(data, colWidths=widths, repeatRows=1)
        sty = [("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#37474f")), ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
               ("GRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#b0bec5")), ("VALIGN", (0, 0), (-1, -1), "TOP"),
               ("LEFTPADDING", (0, 0), (-1, -1), 3), ("RIGHTPADDING", (0, 0), (-1, -1), 3)]
        for k in range(1, len(data)):
            if k % 2 == 0:
                sty.append(("BACKGROUND", (0, k), (-1, k), colors.HexColor("#f5f7f8")))
            if color_col is not None:
                v = str(rows[k][color_col]).split()[0] if str(rows[k][color_col]).strip() else ""
                if v in FILL and FILL[v]:
                    sty.append(("BACKGROUND", (color_col, k), (color_col, k), colors.HexColor(FILL[v])))
        # header text white
        for c in range(len(rows[0])):
            data[0][c].style = ParagraphStyle("hdr", parent=st, textColor=colors.white)
        t.setStyle(TableStyle(sty))
        return t
    asof = ctx.get("as_of", d["built"])
    disclaimer = "Statistical analysis to support your decisions, not a guarantee. Bet only what you can afford to lose. Problem gambling help: 1-800-GAMBLER."
    def footer(c, doc):
        c.saveState(); c.setFont(font, 7); c.setFillColor(colors.grey)
        c.drawString(36, 20, f"Page {doc.page}  |  Data as of {asof}  |  {disclaimer}")
        c.restoreState()
    fname = f"{A}-at-{H}_Week{i['week']}_{i['date']}_Prop_Report.pdf"
    os.makedirs(a.out, exist_ok=True)
    path = os.path.join(a.out, fname)
    doc = SimpleDocTemplate(path, pagesize=landscape(letter), leftMargin=36, rightMargin=36, topMargin=36, bottomMargin=36)
    S = []
    im, fav, dog = R["implied"], R["fav"], R["dog"]
    tn = {A: A, H: H}
    wx = ctx.get("weather") or ("Dome" if i["roof"] in ("dome", "closed") else (f"{i['temp']}F, wind {i['wind']} mph" if i["temp"] is not None else "N/A (not found)"))
    splits = {}
    for p in R["active"]:
        splits[p["l10_split"]] = splits.get(p["l10_split"], 0) + 1
    common = max(splits, key=splits.get) if splits else "N/A"
    S.append(P(f"{i['away']} @ {i['home']} - Player Prop Report", h1))
    S.append(P(f"Kickoff: {i['weekday']}, {i['date']}, {i['time_et']} ET  |  Venue: {i['stadium']} ({i['roof']}, {i['surface']})  |  Weather: {wx}  |  Divisional: {'Yes' if i['div_game'] else 'No'}  |  Rest: {A} {i['away_rest']}d / {H} {i['home_rest']}d"))
    S.append(P(f"Spread: {fav} -{R['sp_abs']}  |  Total: {R['total']}  |  Implied totals: {A} {im[A]:.2f} / {H} {im[H]:.2f}  |  Lines source: {ctx.get('lines_source', 'nflverse schedule + pasted/fetched props')}"))
    S.append(P(f"Data as of: {asof}  |  L10 window: {common} for most players (exceptions in Data Notes)"))

    # ---------- 1 bottom line
    top3 = R["ranked"][:3]
    def pdesc(z):
        return f"{z['p']['name']} {MKT_LABEL[z['m']]} {z['lean']} {z['line']:g} (T{z['tier']})"
    spot_txt = ", ".join(f"{t} {g}" for t, g in sorted(R["spot"])) or "none"
    auto_bl = (f"{fav} favored by {R['sp_abs']}; base script is a {'one-score' if R['sp_abs'] < 7 else 'two-score'} game with implied totals {A} {im[A]:.1f} / {H} {im[H]:.1f}. "
               f"Usage Spotlight: {spot_txt}. Top props: {'; '.join(pdesc(z) for z in top3) or 'none cleared the threshold'}. "
               f"Top TD picks: {', '.join(z['p']['name'] + ' ' + str(z['odds']) + ' (' + z['label'] + ')' for z in R['td_picks'][:3]) or 'none with +4 edge'}.")
    S.append(P("1. Bottom Line", h2))
    S.append(P((ctx.get("bottom_line", "") + " " + auto_bl).strip()))

    # ---------- 2 prop board
    S.append(P("2. Ranked Prop Board", h2))
    rows = [["Rank", "Player", "Prop", "Line", "L10 Avg", "L10 Med", "L10 Hit", "H2H Avg (n)", "Proj", "vs L10", "Lean", "Score", "Tier", "Spotlight"]]
    for k, z in enumerate(R["ranked"], 1):
        h = z["h2h"]
        rows.append([k, f"{z['p']['name']} ({z['p']['team']})", MKT_LABEL[z["m"]], f"{z['line']:g}", fmt(z["avg"]), fmt(z["med"]),
                     z["hit"] if z["lean"] == "Over" else z["hit_u"] + " u", f"{fmt(h['h2h_avg'])} ({h['n']})" if h else "-",
                     fmt(z["proj"]), z["vs"], z["lean"], f"{z['score']}/25", str(z["tier"]), "Yes" if z["spot"] else "-"])
    if len(rows) == 1:
        rows.append(["-", "No prop cleared Tier 3 (score 13+)", "", "", "", "", "", "", "", "", "", "", "", ""])
    S.append(T(rows, [3, 12, 7, 4, 5, 5, 5, 6, 5, 5, 4, 4, 3, 5], fs=8, color_col=12))
    notes = []
    for k, z in enumerate(R["ranked"], 1):
        pr = z["parts"]; p = z["p"]
        base = p["meta"].get("base", {}).get(z["m"])
        h2txt = f" Base {base:.1f} + H2H {z['proj'] - base:+.1f} = {z['proj']:.1f}." if base is not None else ""
        why = (f"{k}. {p['name']} {MKT_LABEL[z['m']]}: {z['label']} matchup, projected volume {p['meta']['vol'].get(z['m'], 0):.1f} vs L10 {fmt(p['meta']['vol_base'].get(z['m']))}"
               f"{', spotlight group' if z['spot'] else ''}; edge {z['edge']:+.1f} ({z['edge_pct']*100:+.0f}%){', P(over) ' + format(z['p_over']*100, '.0f') + '%' if z['p_over'] is not None else ''}. Score parts U{pr['usage']}/M{pr['match']}/S{pr['rob']}/V{pr['lv']}/C{pr['cons']}/P{pr['pers']}"
               f"{'; caps: ' + ', '.join(z['caps']) if z['caps'] else ''}{'; H2H conflict -1' if z['h2h_conf'] else ''}.{h2txt}")
        why += " " + ctx.get("prop_notes", {}).get(f"{p['name']}|{z['m']}", "")
        notes.append(P(why.strip(), small))
    S += notes

    # ---------- 3 TD board
    tt = R["td_team"]
    S.append(P("3. Touchdown Scorer Board", h2))
    S.append(P(f"Expected team TDs: {A} {tt[A]['exp']:.2f} (rush {tt[A]['rush']:.2f} / pass {tt[A]['pass']:.2f}) | {H} {tt[H]['exp']:.2f} (rush {tt[H]['rush']:.2f} / pass {tt[H]['pass']:.2f}). "
               f"Adjustments: {A}: {', '.join(tt[A]['notes']) or 'none'}; {H}: {', '.join(tt[H]['notes']) or 'none'}."))
    rows = [["Rank", "Player", "Team", "RZ share (car / tgt)", "L10 TDs (games)", "Exp TDs", "Model %", "Odds", "Book %", "Edge", "Label", "vs L10"]]
    board = R["td_picks"] if R["td_picks"] else []
    for k, z in enumerate(board, 1):
        p = z["p"]
        rows.append([k, p["name"], p["team"], f"{p['proj_rzr']*100:.0f}% / {p['proj_rzt']*100:.0f}%", f"{p['tds']['total']} ({p['tds']['games_with']}/10)",
                     f"{z['exp']:.2f}", f"{z['prob']*100:.0f}%", z["odds"], f"{z['book']*100:.0f}%", f"{z['edge']:+.1f}", z["label"], z["verdict"]])
    if len(rows) == 1:
        rows.append(["-", "No TD pick cleared the +4 edge threshold" + ("" if any(z["odds"] for z in R["td_rows"]) else " (no TD odds supplied)"), "", "", "", "", "", "", "", "", "", ""])
    S.append(T(rows, [3, 12, 4, 8, 7, 5, 5, 5, 5, 4, 6, 5], fs=8, color_col=10))
    for z in board:
        p = z["p"]
        S.append(P(f"{p['name']}: RZ L10 {p['rz'].get('rz_car', 0)} carries ({p['rz'].get('i10_car', 0)} inside 10) / {p['rz'].get('rz_tgt', 0)} targets ({p['rz'].get('i10_tgt', 0)} inside 10); "
                   f"{R['lab'](p['team'], 'Run game' if p['proj_rzr'] > p['proj_rzt'] else 'Pass efficiency')} matchup{'; ' + z['note'] if z['note'] else ''}.", small))
    fl = [z for z in R["td_rows"] if z["first"] and z["first"]["model"] > (z["first"]["book"] or 1)]
    if fl:
        S.append(P("First TD longshots: " + "; ".join(f"{z['p']['name']} {z['first']['odds']} (model {z['first']['model']*100:.1f}% vs book {z['first']['book']*100:.1f}%)" for z in sorted(fl, key=lambda z: -(z['first']['model'] - z['first']['book']))[:2]), small))
    if R["td_avoid"]:
        S.append(P("TD Avoids: " + "; ".join(f"{z['p']['name']} {z['odds']} (model {z['prob']*100:.0f}% vs book {z['book']*100:.0f}%)" for z in R["td_avoid"]), small))

    # ---------- 4 spotlight
    S.append(P("4. Usage Spotlight", h2))
    if R["spot"]:
        for (t, g) in sorted(R["spot"]):
            mem = [p["name"] for p in R["active"] if p["team"] == t and ((g == "QB passing" and p["pos"] == "QB") or (g == p["pos"]) or (g.startswith("RB") and p["pos"] == "RB"))]
            S.append(P(f"{t} {g}: criteria - {'; '.join(R['groups'][(t, g)])}. Beneficiaries: {', '.join(mem)}. " + ctx.get("spotlight_text", {}).get(f"{t}:{g}", "")))
    else:
        S.append(P("No position group met two or more spotlight criteria. Near misses: " + "; ".join(f"{t} {g} ({c[0]})" for (t, g), c in R["groups"].items() if len(c) == 1) or "none"))

    # ---------- 5 H2H
    S.append(P("5. Head-to-Head Trends", h2))
    rows = [["Player", "Metric", "H2H Avg (n)", "Season Avg", "H2H Delta", "Above/Below", "Context", "Weight Used"]]
    for p in R["active"]:
        h = p.get("h2h")
        if not h:
            continue
        for m, x in h["metrics"].items():
            if m not in H2H_METRICS and m != "td":
                continue
            u = p["h2h_used"].get(m)
            ctxs = f"diff QB {h['diff_qb']}/{h['n']}, diff team {h['diff_team']}/{h['n']}" + (f"; excl {', '.join(h['excluded'])}" if h["excluded"] else "")
            rows.append([p["name"], MKT_LABEL.get(m, m), f"{fmt(x['h2h_avg'])} ({h['n']})", fmt(x["season_avg"]), f"{x['delta']:+.1f}",
                         f"{x['above']}/{x['below']}", ctxs, f"{u['weight']*100:.0f}%" if u and u.get("qual") else "not qualified"])
    if len(rows) > 1:
        S.append(T(rows, [10, 7, 6, 6, 5, 5, 16, 6], fs=8))
    else:
        S.append(P("No in-scope player has meetings with this opponent in the last 3 seasons."))
    ht = d.get("h2h_team", {})
    if ht:
        S.append(P("Team pattern: " + "; ".join(f"{t} pass rate {v['pass_rate_h2h']}% in {v['n']} meetings vs {v['pass_rate_season']}% in those seasons (higher in {v['higher_in']}/{v['n']})" for t, v in ht.items()) + ". " + ctx.get("h2h_text", ""), small))

    # ---------- 6 game script
    S.append(P("6. Game Script Simulation", h2))
    for t in (A, H):
        S.append(P(f"{t} equilibrium plan: {ctx.get('equilibrium', {}).get(t, 'N/A (analyst input not provided)')}"))
    S.append(P(f"Possession strategy: {ctx.get('possession', 'N/A (analyst input not provided)')}"))
    sc = ctx.get("scenarios") or default_scenarios(d, R)
    S.append(T([["Scenario", "Prob", "Score range", f"Plays ({A}/{H})", "Tilt", "Benefits", "Hurts"]] + sc, [8, 4, 9, 6, 8, 14, 12], fs=8.5))
    rows = [["Team", "Proj. Plays", "Proj. Pass Att", "Proj. Rush Att", "Dropback rate", "Sack rate used"]]
    for t in (A, H):
        v = R["vol"][t]
        rows.append([t, f"{v['plays']:.1f}", f"{v['pass_att']:.1f}", f"{v['rush_att']:.1f}", f"{v['db_rate']*100:.1f}%", f"{v['sack']*100:.1f}%"])
    S.append(Spacer(1, 4)); S.append(T(rows, [4, 5, 5, 5, 5, 5], fs=8.5))
    rob = [f"{z['p']['name']} {MKT_LABEL[z['m']]}" for z in R["ranked"] if z["parts"]["rob"] >= 3]
    dep = [f"{z['p']['name']} {MKT_LABEL[z['m']]}" for z in R["ranked"] if z["parts"]["rob"] <= 1]
    S.append(P(f"Script-robust props: {', '.join(rob) or 'none'}  |  Script-dependent props: {', '.join(dep) or 'none'}", small))

    # ---------- 7 matchup matrix
    S.append(PageBreak())
    S.append(P("7. Matchup Matrix", h2))
    for t in (A, H):
        o = t; df = H if t == A else A
        S.append(P(f"{o} offense vs {df} defense", h3))
        rows = [["Lane", "Offense (value, rank)", "Defense allowed (value, rank)", "Tiers", "Label"]]
        for x in d["matchups"][t]:
            lb = R["labels"][(t, x["lane"])]
            rows.append([x["lane"], f"{fmt(x['off'], 2)} (#{x['off_rank']})", f"{fmt(x['def'], 2)} (#{x['def_rank']})", f"{x['off_tier']} vs {x['def_tier']}",
                         lb + (" (override)" if lb != x["label"] else "")])
        S.append(T(rows, [8, 8, 9, 10, 6], fs=8.5))
        up = [x["lane"] for x in d["matchups"][t] if R["labels"][(t, x["lane"])] in ("ATTACK", "LEAN ATTACK")]
        dn = [x["lane"] for x in d["matchups"][t] if R["labels"][(t, x["lane"])] in ("FADE", "LEAN FADE")]
        ct = [x["lane"] for x in d["matchups"][t] if R["labels"][(t, x["lane"])] == "CONTESTED"]
        S.append(P(f"Upside lanes: {', '.join(up) or 'none'}  |  Downside lanes: {', '.join(dn) or 'none'}  |  Contested: {', '.join(ct) or 'none'}  |  "
                   f"Redistribution call: {ctx.get('redistribution', {}).get(t, 'N/A (analyst input not provided)')}", small))
        tw = d["teams"][t]["off"].get("w_cur")
        S.append(P(f"Ranks use a blend of {i['season']} and {i['season'] - 1} ({(tw or 0)*100:.0f}% current-season weight for {t}).", small))

    # ---------- 8 personnel
    S.append(P("8. Personnel and Coaching", h2))
    rows = [["Player", "Team", "Pos", "Status", "Practice", "Impact", "Who benefits"]]
    for nm, s in R["status"].items():
        if s.get("status") in (None, "", "None") and not s.get("impact"):
            continue
        imp = s.get("impact", "")
        ben = s.get("benefits", "")
        if not imp:
            v = next((v for v in R["vac"] if v["name"] == nm), None)
            if v:
                imp = f"Vacates {((v['target_share'] or 0)*100):.0f}% targets / {((v['carry_share'] or 0)*100):.0f}% carries (reallocated proportionally, estimate)"
                ben = ", ".join(p["name"] for p in R["active"] if p["team"] == v["team"] and p.get("vac_bump"))
            elif s.get("status") == "Questionable":
                imp = "Caps related props at Tier 2 / TD label at Value"
        rows.append([nm, s.get("team", ""), s.get("pos", ""), s.get("status", ""), s.get("practice", ""), imp, ben])
    S.append(T(rows, [9, 3, 3, 5, 10, 16, 10], fs=8.5))
    for t in (A, H):
        T0 = d["teams"][t]["off"]
        auto = (f"Neutral pass rate {fmt(T0['neutral_pass_rate']['v'])}% (#{T0['neutral_pass_rate']['rank']}), PROE {fmt(T0['proe']['v'])}, pace {fmt(T0.get('pace_sec', {}).get('v'))} s/play, "
                f"{fmt(T0['plays_pg']['v'])} plays/g. Coach: {i['away_coach'] if t == A else i['home_coach']}.")
        S.append(P(f"{t} coaching profile: {auto} {ctx.get('coaching', {}).get(t, '')}", small))

    # ---------- 9 grid
    S.append(PageBreak())
    S.append(P("9. Full Player Metric Grid", h2))
    for t in (A, H):
        S.append(P(t, h3))
        rows = [["Player", "Metric", "L10 Avg", "L10 Med", "Last-3", "H2H Avg (n)", "Projection", "Verdict"]]
        for p in [q for q in R["active"] if q["team"] == t]:
            for m in {"QB": ["pass_att", "cmp", "pass_yds", "pass_td", "int", "rush_att", "rush_yds", "long_comp", "td"],
                      "RB": ["rush_att", "rush_yds", "targets", "rec", "rec_yds", "rush_rec_yds", "td"],
                      "WR": ["targets", "rec", "rec_yds", "long_rec", "td"], "TE": ["targets", "rec", "rec_yds", "long_rec", "td"]}[p["pos"]]:
                mm = p["metrics"].get(m)
                pr = p["proj"].get(m)
                h = (p.get("h2h") or {}).get("metrics", {}).get(m)
                if m == "td":
                    avg = p["tds"]["pg"]
                    vv = "N/A" if pr is None else ("AT" if abs(pr - avg) <= .15 else ("ABOVE" if pr > avg else "BELOW"))
                else:
                    avg = mm["avg"] if mm else None
                    vv = R["verdict"](pr, avg)
                rows.append([f"{p['name']} ({p['pos']})", MKT_LABEL.get(m, m), fmt(avg, 2 if m in ('td', 'pass_td', 'int') else 1),
                             fmt(mm["med"]) if mm else "N/A", mm["trend"] if mm else "N/A",
                             f"{fmt(h['h2h_avg'])} ({p['h2h']['n']})" if h else "-", fmt(pr, 2 if m in ('td', 'pass_td', 'int') else 1), vv])
        S.append(T(rows, [12, 7, 5, 5, 4, 6, 5, 5], fs=8.5))

    # ---------- 10 fades
    S.append(P("10. Fades and Avoids", h2))
    fl = [f"{z['p']['name']} {MKT_LABEL[z['m']]} Under {z['line']:g}: proj {z['proj']:.1f}, {z['label']} lane, L10 under {z['hit_u']}." for z in R["fades"]]
    fl += [f"{z['p']['name']} {MKT_LABEL[z['m']]} {z['line']:g}: no edge (proj {z['proj']:.1f}, L10 med {fmt(z['med'])})." for z in R["props"] if z["lean"] == "Pass"][:6]
    fl += ctx.get("fades", [])
    for x in fl or ["None."]:
        S.append(P("- " + x, small))

    # ---------- 11 red flags
    S.append(P("11. Red Flags / What Changes the Read", h2))
    rf = list(ctx.get("red_flags", []))
    for nm, s in R["status"].items():
        if s.get("status") == "Questionable":
            aff = [f"{z['p']['name']} {MKT_LABEL[z['m']]}" for z in R["ranked"] if z["p"]["team"] == s.get("team")]
            rf.append(f"If {nm} ({s.get('team')}) is inactive: rerun report with status Out; re-check {', '.join(aff[:4]) or 'team props'} (volume redistributes).")
    if not rf:
        rf = ["No unresolved tags. Check official inactives 90 minutes before kickoff."]
    for x in rf:
        S.append(P("- " + x, small))

    # ---------- 12 correlation
    S.append(P("12. Correlation Notes", h2))
    cn = list(ctx.get("correlation", []))
    rk = R["ranked"]
    for x in rk:
        for y in rk:
            if x is y or x["p"]["team"] != y["p"]["team"]:
                continue
            if x["m"] in ("pass_yds", "pass_att") and y["m"] in ("rec_yds", "rec") and x["lean"] == y["lean"] == "Over":
                cn.append(f"Pairs: {x['p']['name']} {MKT_LABEL[x['m']]} Over + {y['p']['name']} {MKT_LABEL[y['m']]} Over (same passing script).")
            if x["m"] in ("rush_att", "rush_yds") and y["m"] in ("pass_att", "pass_yds") and x["lean"] == y["lean"] == "Over":
                cn.append(f"Contradiction: {x['p']['name']} {MKT_LABEL[x['m']]} Over and {y['p']['name']} {MKT_LABEL[y['m']]} Over need different scripts.")
    for z in R["td_picks"]:
        for y in R["props"]:
            if y["p"] is z["p"] and y["lean"] == "Under" and y["tier"] > 0:
                cn.append(f"Contradiction: {z['p']['name']} TD pick vs {MKT_LABEL[y['m']]} Under.")
            if y["p"] is z["p"] and y["lean"] == "Over" and y["tier"] > 0:
                cn.append(f"Pairs: {z['p']['name']} anytime TD + {MKT_LABEL[y['m']]} Over.")
    for x in list(dict.fromkeys(cn)) or ["No notable pairings among ranked props."]:
        S.append(P("- " + x, small))

    # ---------- 13 data notes
    S.append(P("13. Data Notes", h2))
    dn = [f"Source: nflverse (play-by-play, weekly player stats, snap counts, rosters, injuries, schedule) built {d['built']}; current-season pbp through week {i['data_through']['pbp_max_week_cur']}.",
          "Pressure rate, blitz rate, man/zone and shell rates, and personnel-grouping rates are not in nflverse: N/A (not found) unless supplied in analyst notes. TE2 inclusion uses snap share 40%+ as a proxy for 12 personnel.",
          "Injury-exit exclusion: games under 50% of the player's recent median snap share are dropped from L10 and replaced by the next older game."]
    for p in R["active"]:
        extra = []
        if p["l10_split"] != common:
            extra.append(f"L10 {p['l10_split']}")
        if p["excluded"]:
            extra.append("excluded " + "; ".join(p["excluded"]))
        if p["flags"]:
            extra.append("flag: " + ", ".join(p["flags"]) + " (current-season games weighted 2x in projections)")
        if any(g["po"] for g in p["log"]):
            extra.append("includes playoff games")
        if extra:
            dn.append(f"{p['name']}: " + " | ".join(extra))
    if R["unmatched"]:
        dn.append("Lines not matched to an in-scope player/market: " + ", ".join(R["unmatched"][:15]))
    dn += d.get("warnings", []) + ctx.get("data_notes", [])
    for x in dn:
        S.append(P("- " + x, small))
    S.append(Spacer(1, 8))
    S.append(P("This is statistical analysis to support your decisions, not a guarantee. Even strong edges lose often, and touchdown bets are especially volatile. Bet only what you can afford to lose. Problem gambling help: 1-800-GAMBLER.", small))
    doc.build(S, onFirstPage=footer, onLaterPages=footer)

    # ---------- verify + chat summary
    txt = subprocess.run(["pdftotext", "-layout", path, "-"], capture_output=True, text=True).stdout
    need = ["1. Bottom Line", "2. Ranked Prop Board", "3. Touchdown Scorer Board", "4. Usage Spotlight", "5. Head-to-Head",
            "6. Game Script", "7. Matchup Matrix", "8. Personnel", "9. Full Player Metric Grid", "10. Fades", "11. Red Flags",
            "12. Correlation", "13. Data Notes"]
    missing = [s for s in need if s not in txt]
    boxes = txt.count("\u25a0") + txt.count("\ufffd")
    print(f"PDF: {path}\nVERIFY: sections missing={missing or 'none'} black-box chars={boxes} pages={txt.count(chr(12))}")
    print(f"SUMMARY: {A}@{H} {i['weekday']} {i['time_et']} ET | {fav} -{R['sp_abs']} / {R['total']} | implied {A} {im[A]:.2f} {H} {im[H]:.2f}")
    for z in R["ranked"][:3]:
        print(f"TOP: {pdesc(z)} proj {z['proj']:.1f} score {z['score']}")
    for z in R["td_picks"][:3]:
        print(f"TD: {z['p']['name']} {z['odds']} {z['label']} model {z['prob']*100:.0f}% edge {z['edge']:+.1f}")
    if R["unmatched"]:
        print("UNMATCHED LINES:", ", ".join(R["unmatched"]))

# ----------------------------------------------------------------------------- main
def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    d = sub.add_parser("data"); d.add_argument("--away", required=True); d.add_argument("--home", required=True)
    d.add_argument("--season", type=int, required=True); d.add_argument("--week", type=int, required=True)
    d.add_argument("--work", default="/home/claude/work")
    l = sub.add_parser("lines"); l.add_argument("--work", default="/home/claude/work"); l.add_argument("--api-key", required=True)
    l.add_argument("--book", default="fanduel")
    rp = sub.add_parser("report"); rp.add_argument("--work", default="/home/claude/work"); rp.add_argument("--lines")
    rp.add_argument("--context"); rp.add_argument("--out", default="/mnt/user-data/outputs")
    a = ap.parse_args()
    {"data": cmd_data, "lines": cmd_lines, "report": cmd_report}[a.cmd](a)

if __name__ == "__main__":
    main()
