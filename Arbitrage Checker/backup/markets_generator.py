import json
from typing import Dict, List, Any


def generate_market_sets(
        base_markets: Dict[str, List[str]],
        over_under_config: Dict[str, Any],
        handicap_config: Dict[str, Any],
        home_team_ou_config: Dict[str, Any],
        away_team_ou_config: Dict[str, Any],
        first_half_ou_config: Dict[str, Any],
        corners_ou_config: Dict[str, Any],
        home_both_halves_config: Dict[str, Any],
        away_both_halves_config: Dict[str, Any],
        home_second_half_config: Dict[str, Any],
        away_second_half_config: Dict[str, Any]
) -> Dict[str, List[str]]:
    """
    Generate a complete market_sets dict based on provided configs.
    """
    markets = base_markets.copy()

    def fmt(n: float) -> str:
        s = f"{n:.2f}".rstrip('0').rstrip('.')
        if '.' not in s:
            s += '.0'
        return s

    # Home Team Score/No Score Both Halves
    markets[home_both_halves_config['template']] = [
        home_both_halves_config['score_tpl'],
        home_both_halves_config['noscore_tpl']
    ]

    # Away Team Score/No Score Both Halves
    markets[away_both_halves_config['template']] = [
        away_both_halves_config['score_tpl'],
        away_both_halves_config['noscore_tpl']
    ]

    # Home Team Score/No Score Second Half
    markets[home_second_half_config['template']] = [
        home_second_half_config['score_tpl'],
        home_second_half_config['noscore_tpl']
    ]

    # Away Team Score/No Score Second Half
    markets[away_second_half_config['template']] = [
        away_second_half_config['score_tpl'],
        away_second_half_config['noscore_tpl']
    ]

    # Total Over/Under
    u = over_under_config
    n = u['start']
    while n <= u['end'] + 1e-9:
        ns = fmt(n)
        markets[u['template'].format(n=ns)] = [u['home_tpl'].format(n=ns), u['away_tpl'].format(n=ns)]
        n += u['step']

    # Home Team Over/Under
    htu = home_team_ou_config
    n = htu['start']
    while n <= htu['end'] + 1e-9:
        ns = fmt(n)
        markets[htu['template'].format(n=ns)] = [htu['home_tpl'].format(n=ns), htu['away_tpl'].format(n=ns)]
        n += htu['step']

    # Away Team Over/Under
    atu = away_team_ou_config
    n = atu['start']
    while n <= atu['end'] + 1e-9:
        ns = fmt(n)
        markets[atu['template'].format(n=ns)] = [atu['home_tpl'].format(n=ns), atu['away_tpl'].format(n=ns)]
        n += atu['step']

    # First Half Over/Under
    fhu = first_half_ou_config
    n = fhu['start']
    while n <= fhu['end'] + 1e-9:
        ns = fmt(n)
        markets[fhu['template'].format(n=ns)] = [fhu['home_tpl'].format(n=ns), fhu['away_tpl'].format(n=ns)]
        n += fhu['step']

    # Total Corners Over/Under
    cu = corners_ou_config
    n = cu['start']
    while n <= cu['end'] + 1e-9:
        ns = fmt(n)
        markets[cu['template'].format(n=ns)] = [cu['home_tpl'].format(n=ns), cu['away_tpl'].format(n=ns)]
        n += cu['step']


    # Handicap
    h = handicap_config
    v = h['start']
    while v <= h['end'] + 1e-9:
        ns = fmt(abs(v))
        sign = '-' if v < 0 else ''
        opp = '' if v < 0 else '-'
        markets[h['template'].format(sign=sign, n=ns)] = [
            h['home_tpl'].format(sign=sign, n=ns),
            h['away_tpl'].format(sign=opp, n=ns)
        ]
        v += h['step']

    return markets


if __name__ == '__main__':
    # configs
    # Main lines
    base = {
        "three_way": ["1_odd", "draw_odd", "2_odd"],
        "one_vs_x2": ["1_odd", "X2_odd"],
        "two_vs_1x": ["2_odd", "1X_odd"],
        "x_vs_12": ["draw_odd", "12_odd"],
        "both_score": ["both_score_odd", "both_noscore_odd"],
    }

    # Home Team Score/No Score Both Halves
    home_both_halves = {'template': 'home_score_both_halves_vs_home_noscore_both_halves',
                        'score_tpl': 'home_score_both_halves_odd',
                        'noscore_tpl': 'home_noscore_both_halves_odd'
                        }

    # Away Team Score/No Score Both Halves
    away_both_halves = {'template': 'away_score_both_halves_vs_away_noscore_both_halves',
                        'score_tpl': 'away_score_both_halves_odd',
                        'noscore_tpl': 'away_noscore_both_halves_odd'
                        }

    # Home Team Score/No Score Second Half
    home_second_half = {'template': 'home_score_second_half_vs_home_noscore_second_half',
                        'score_tpl': 'home_score_second_half_odd',
                        'noscore_tpl': 'home_noscore_second_half_odd'
                        }

    # Away Team Score/No Score Second Half
    away_second_half = {'template': 'away_score_second_half_vs_away_noscore_second_half',
                        'score_tpl': 'away_score_second_half_odd',
                        'noscore_tpl': 'away_noscore_second_half_odd'
                        }

    # Total Over/Under lines
    ou = {'start': 0.5,
          'end': 8.5,
          'step': 0.25,
          'template': 'under_{n}_vs_over_{n}',
          'home_tpl': 'under_{n}_odd',
          'away_tpl': 'over_{n}_odd'
          }

    # Home Team Over/Under lines
    home_team_ou = {'start': 0.5,
                    'end': 8.5,
                    'step': 0.25,
                    'template': 'home_under_{n}_vs_home_over_{n}',
                    'home_tpl': 'home_under_{n}_odd',
                    'away_tpl': 'home_over_{n}_odd'
                    }

    # Away Team Over/Under lines
    away_team_ou = {'start': 0.5,
                    'end': 8.5,
                    'step': 0.5,
                    'template': 'away_under_{n}_vs_away_over_{n}',
                    'home_tpl': 'away_under_{n}_odd',
                    'away_tpl': 'away_over_{n}_odd'
                    }

    # First Half Over/Under lines
    first_half_ou = {'start': 0.5,
                     'end': 8.5,
                     'step': 0.5,
                     'template': 'first_half_under_{n}_vs_first_half_over_{n}',
                     'home_tpl': 'first_half_under_{n}_odd',
                     'away_tpl': 'first_half_over_{n}_odd'
                     }

    # Total Corners Over/Under lines
    corners_ou = {'start': 0.5,
                  'end': 15.5,
                  'step': 0.5,
                  'template': 'corners_under_{n}_vs_corners_over_{n}',
                  'home_tpl': 'corners_under_{n}_odd',
                  'away_tpl': 'corners_over_{n}_odd'
                  }

    # Handicap lines
    ah = {'start': -8.5,
          'end': 8.5,
          'step': 0.25,
          'template': 'ah_{sign}{n}_home_vs_away',
          'home_tpl': 'home_handicap_{sign}{n}_odd',
          'away_tpl': 'away_handicap_{sign}{n}_odd'
          }

    ms = generate_market_sets(
        base, ou, ah, home_team_ou, away_team_ou, first_half_ou, corners_ou,
        home_both_halves, away_both_halves, home_second_half, away_second_half
    )

    # write compact with inline arrays
    items = list(ms.items())
    with open('markets.json', 'w', encoding='utf-8') as f:
        f.write('{' + '"market_sets": {')
        for i, (k, v) in enumerate(items):
            comma = ',' if i < len(items) - 1 else ''
            f.write(f'  "{k}": {json.dumps(v)}{comma}\n')
        f.write('}}')
    print("markets.json generated in compact form.")