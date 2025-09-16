import json
from typing import Dict, List, Any


def generate_market_sets(market_definitions: Dict[str, Any], enabled_markets: Dict[str, bool]) -> Dict[str, List[str]]:
    """
    Generate a complete market_sets dict based on market definitions and enabled markets.
    """
    markets = {}

    def fmt(n: float) -> str:
        s = f"{n:.2f}".rstrip('0').rstrip('.')
        if '.' not in s:
            s += '.0'
        return s

    for market_name, config in market_definitions.items():
        if not enabled_markets.get(market_name, True):
            continue

        market_type = config.get('type', 'simple')

        if market_type == 'simple':
            # Simple markets with fixed outcomes
            if 'outcomes' in config:
                outcomes = config['outcomes']
            else:
                # Support legacy binary format
                outcomes = [config['positive_tpl'], config['negative_tpl']]
            markets[config['template']] = outcomes

        elif market_type == 'range':
            # Markets with numeric ranges (over/under, handicap, etc.)
            n = config['start']
            while n <= config['end'] + 1e-9:
                ns = fmt(n)

                if config.get('use_sign', False):
                    # zero-fix for handicap markets
                    if abs(n) < 1e-9:
                        sign = ''
                        opp_sign = ''
                    else:
                        sign = '-' if n < 0 else ''
                        opp_sign = '' if n < 0 else '-'
                    abs_n = fmt(abs(n))
                    market_key = config['template'].format(sign=sign, n=abs_n)
                    outcomes = [
                        config['home_tpl'].format(sign=sign, n=abs_n),
                        config['away_tpl'].format(sign=opp_sign, n=abs_n)
                    ]
                else:
                    # For over/under markets
                    market_key = config['template'].format(n=ns)
                    outcomes = [
                        config['home_tpl'].format(n=ns),
                        config['away_tpl'].format(n=ns)
                    ]

                markets[market_key] = outcomes
                n += config['step']

    return markets


if __name__ == '__main__':
    # =================================================================
    # MARKET DEFINITIONS - Add new markets here easily
    # =================================================================
    MARKET_DEFINITIONS = {
        # Simple fixed markets
        'three_way': {
            'type': 'simple',
            'template': 'three_way',
            'outcomes': ["1_odd", "draw_odd", "2_odd"]
        },
        'one_vs_x2': {
            'type': 'simple',
            'template': 'one_vs_x2',
            'outcomes': ["1_odd", "X2_odd"]
        },
        'two_vs_1x': {
            'type': 'simple',
            'template': 'two_vs_1x',
            'outcomes': ["2_odd", "1X_odd"]
        },
        'x_vs_12': {
            'type': 'simple',
            'template': 'x_vs_12',
            'outcomes': ["draw_odd", "12_odd"]
        },

        # <-- Inserted markets: these will be generated immediately after x_vs_12 -->
        'homewin_handicap_vs_x2': {
            'type': 'simple',
            'template': 'homewin_handicap_vs_x2',
            'outcomes': ["home_handicap_-0.5_odd", "X2_odd"]
        },
        'awaywin_handicap_vs_1x': {
            'type': 'simple',
            'template': 'awaywin_handicap_vs_1x',
            'outcomes': ["away_handicap_-0.5_odd", "1X_odd"]
        },
        'one_vs_x2_handicap': {
            'type': 'simple',
            'template': 'one_vs_x2_handicap',
            'outcomes': ["1_odd", "away_handicap_0.5_odd"]
        },
        'two_vs_1x_handicap': {
            'type': 'simple',
            'template': 'two_vs_1x_handicap',
            'outcomes': ["2_odd", "home_handicap_0.5_odd"]
        },
        # <-- End inserted markets -->

        'home_qualify_vs_away_qualify': {
            'type': 'simple',
            'template': 'home_qualify_vs_away_qualify',
            'outcomes': ["home_qualify_odd", "away_qualify_odd"]
        },
        'both_score': {
            'type': 'simple',
            'template': 'both_score',
            'outcomes': ["both_score_odd", "both_noscore_odd"]
        },

        # Score/No Score markets
        'home_both_halves': {
            'type': 'simple',
            'template': 'home_score_both_halves_vs_home_noscore_both_halves',
            'outcomes': ['home_score_both_halves_odd', 'home_noscore_both_halves_odd']
        },
        'away_both_halves': {
            'type': 'simple',
            'template': 'away_score_both_halves_vs_away_noscore_both_halves',
            'outcomes': ['away_score_both_halves_odd', 'away_noscore_both_halves_odd']
        },
        'home_second_half': {
            'type': 'simple',
            'template': 'home_score_second_half_vs_home_noscore_second_half',
            'outcomes': ['home_score_second_half_odd', 'home_noscore_second_half_odd']
        },
        'away_second_half': {
            'type': 'simple',
            'template': 'away_score_second_half_vs_away_noscore_second_half',
            'outcomes': ['away_score_second_half_odd', 'away_noscore_second_half_odd']
        },
        'penalty': {
            'type': 'simple',
            'template': 'penalty_in_match_odd_vs_no_penalty_in_match_odd',
            'outcomes': ['penalty_in_match_odd', 'no_penalty_in_match_odd']
        },

        # Range markets - Over/Under
        'total_over_under': {
            'type': 'range',
            'start': 0.5,
            'end': 8.5,
            'step': 0.25,
            'template': 'under_{n}_vs_over_{n}',
            'home_tpl': 'under_{n}_odd',
            'away_tpl': 'over_{n}_odd'
        },
        'home_team_over_under': {
            'type': 'range',
            'start': 0.5,
            'end': 8.5,
            'step': 1,
            'template': 'home_under_{n}_vs_home_over_{n}',
            'home_tpl': 'home_under_{n}_odd',
            'away_tpl': 'home_over_{n}_odd'
        },
        'away_team_over_under': {
            'type': 'range',
            'start': 0.5,
            'end': 8.5,
            'step': 1,
            'template': 'away_under_{n}_vs_away_over_{n}',
            'home_tpl': 'away_under_{n}_odd',
            'away_tpl': 'away_over_{n}_odd'
        },
        'first_half_over_under': {
            'type': 'range',
            'start': 0.5,
            'end': 8.5,
            'step': 0.5,
            'template': 'first_half_under_{n}_vs_first_half_over_{n}',
            'home_tpl': 'first_half_under_{n}_odd',
            'away_tpl': 'first_half_over_{n}_odd'
        },
        'second_half_over_under': {
            'type': 'range',
            'start': 0.5,
            'end': 8.5,
            'step': 0.5,
            'template': 'second_half_under_{n}_vs_second_half_over_{n}',
            'home_tpl': 'second_half_under_{n}_odd',
            'away_tpl': 'second_half_over_{n}_odd'
        },
        'home_first_half_over_under': {
            'type': 'range',
            'start': 0.5,
            'end': 8.5,
            'step': 0.5,
            'template': 'home_first_half_under_{n}_vs_home_first_half_over_{n}',
            'home_tpl': 'home_first_half_under_{n}_odd',
            'away_tpl': 'home_first_half_over_{n}_odd'
        },
        'away_first_half_over_under': {
            'type': 'range',
            'start': 0.5,
            'end': 8.5,
            'step': 0.5,
            'template': 'away_first_half_under_{n}_vs_away_first_half_over_{n}',
            'home_tpl': 'away_first_half_under_{n}_odd',
            'away_tpl': 'away_first_half_over_{n}_odd'
        },
        'home_second_half_over_under': {
            'type': 'range',
            'start': 0.5,
            'end': 8.5,
            'step': 0.5,
            'template': 'home_second_half_under_{n}_vs_home_second_half_over_{n}',
            'home_tpl': 'home_second_half_under_{n}_odd',
            'away_tpl': 'home_second_half_over_{n}_odd'
        },
        'away_second_half_over_under': {
            'type': 'range',
            'start': 0.5,
            'end': 8.5,
            'step': 0.5,
            'template': 'away_second_half_under_{n}_vs_away_second_half_over_{n}',
            'home_tpl': 'away_second_half_under_{n}_odd',
            'away_tpl': 'away_second_half_over_{n}_odd'
        },
        'corners_over_under': {
            'type': 'range',
            'start': 0.5,
            'end': 15.5,
            'step': 0.5,
            'template': 'corners_under_{n}_vs_corners_over_{n}',
            'home_tpl': 'corners_under_{n}_odd',
            'away_tpl': 'corners_over_{n}_odd'
        },

        # Range markets - Handicap (with signs)
        'handicap': {
            'type': 'range',
            'start': -8.5,
            'end': 8.5,
            'step': 0.25,
            'use_sign': True,
            'template': 'ah_{sign}{n}_home_vs_away',
            'home_tpl': 'home_handicap_{sign}{n}_odd',
            'away_tpl': 'away_handicap_{sign}{n}_odd'
        },
        'first_half_handicap': {
            'type': 'range',
            'start': -8.5,
            'end': 8.5,
            'step': 0.25,
            'use_sign': True,
            'template': 'first_half_ah_{sign}{n}_home_vs_away',
            'home_tpl': 'home_first_half_handicap_{sign}{n}_odd',
            'away_tpl': 'away_first_half_handicap_{sign}{n}_odd'
        },
        'second_half_handicap': {
            'type': 'range',
            'start': -8.5,
            'end': 8.5,
            'step': 0.25,
            'use_sign': True,
            'template': 'second_half_ah_{sign}{n}_home_vs_away',
            'home_tpl': 'home_second_half_handicap_{sign}{n}_odd',
            'away_tpl': 'away_second_half_handicap_{sign}{n}_odd'
        },
    }

    # =================================================================
    # CUSTOMIZATION: Set True/False to enable/disable each market type
    # =================================================================
    ENABLED_MARKETS = {
        'three_way': True,
        'one_vs_x2': True,
        'two_vs_1x': True,
        'x_vs_12': True,
        'home_qualify_vs_away_qualify': True,
        'both_score': True,
        'home_both_halves': True,
        'away_both_halves': True,
        'home_second_half': True,
        'away_second_half': True,
        'penalty': True,
        'total_over_under': True,
        'home_team_over_under': True,
        'away_team_over_under': True,
        'first_half_over_under': True,
        'second_half_over_under': True,
        'home_first_half_over_under': True,
        'away_first_half_over_under': True,
        'home_second_half_over_under': True,
        'away_second_half_over_under': True,
        'corners_over_under': True,
        'handicap': True,
        'first_half_handicap': True,
        'second_half_handicap': True
    }

    ms = generate_market_sets(MARKET_DEFINITIONS, ENABLED_MARKETS)

    # write compact with inline arrays
    items = list(ms.items())
    with open('markets.json', 'w', encoding='utf-8') as f:
        f.write('{' + '"market_sets": {')
        for i, (k, v) in enumerate(items):
            comma = ',' if i < len(items) - 1 else ''
            f.write(f'  "{k}": {json.dumps(v)}{comma}\n')
        f.write('}}')
    print("markets.json generated in compact form.")
