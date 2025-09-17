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
            markets[config['template']] = config['outcomes']

        elif market_type == 'range':
            # Markets with numeric ranges (over/under, handicap, etc.)
            n = config['start']
            while n <= config['end'] + 1e-9:
                ns = fmt(n)

                if config.get('use_sign', False):
                    # For handicap markets. Player 1 is 'home', Player 2 is 'away'.
                    if abs(n) < 1e-9:
                        sign = ''
                        opp_sign = ''
                    else:
                        sign = '-' if n < 0 else ''
                        opp_sign = '' if n < 0 else '-'
                    abs_n = fmt(abs(n))
                    market_key = config['template'].format(sign=sign, n=abs_n)
                    # home_tpl is for Player 1, away_tpl is for Player 2
                    # The opp_sign correctly creates the opposing handicap for Player 2
                    outcomes = [
                        config['home_tpl'].format(sign=sign, n=abs_n),
                        config['away_tpl'].format(sign=opp_sign, n=abs_n)
                    ]
                else:
                    # For over/under markets. 'home' is under, 'away' is over.
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
    # TENNIS MARKET DEFINITIONS - Add new tennis markets here
    # =================================================================
    MARKET_DEFINITIONS = {
        # --- Simple Binary Markets ---
        'winner': {
            'type': 'simple',
            'template': 'winner',
            'outcomes': ["player1_odd", "player2_odd"]
        },
        'set1_winner': {
            'type': 'simple',
            'template': 'set1_winner',
            'outcomes': ["player1_win_set1_odd", "player2_win_set1_odd"]
        },
        'set2_winner': {
            'type': 'simple',
            'template': 'set2_winner',
            'outcomes': ["player1_win_set2_odd", "player2_win_set2_odd"]
        },
        'set1_even_odd': {
            'type': 'simple',
            'template': 'set1_total_games_even_vs_odd',
            'outcomes': ["set1_total_games_even_odd", "set1_total_games_odd_odd"]
        },
        'set2_even_odd': {
            'type': 'simple',
            'template': 'set2_total_games_even_vs_odd',
            'outcomes': ["set2_total_games_even_odd", "set2_total_games_odd_odd"]
        },

        # --- Range Markets - Over/Under ---
        'total_sets_over_under': {
            'type': 'range',
            'start': 2.5,
            'end': 4.5,
            'step': 1.0,
            'template': 'total_sets_under_{n}_vs_over_{n}',
            'home_tpl': 'total_sets_under_{n}_odd', # Under
            'away_tpl': 'total_sets_over_{n}_odd'  # Over
        },
        'total_games_over_under': {
            'type': 'range',
            'start': 16.5,
            'end': 28.5,
            'step': 0.5,
            'template': 'total_games_under_{n}_vs_over_{n}',
            'home_tpl': 'total_games_under_{n}_odd',
            'away_tpl': 'total_games_over_{n}_odd'
        },
        'player1_total_games_over_under': {
            'type': 'range',
            'start': 8.5,
            'end': 15.5,
            'step': 0.5,
            'template': 'player1_total_games_under_{n}_vs_over_{n}',
            'home_tpl': 'player1_total_games_under_{n}_odd',
            'away_tpl': 'player1_total_games_over_{n}_odd'
        },
        'player2_total_games_over_under': {
            'type': 'range',
            'start': 8.5,
            'end': 15.5,
            'step': 0.5,
            'template': 'player2_total_games_under_{n}_vs_over_{n}',
            'home_tpl': 'player2_total_games_under_{n}_odd',
            'away_tpl': 'player2_total_games_over_{n}_odd'
        },
        'set1_total_games_over_under': {
            'type': 'range',
            'start': 6.5,
            'end': 12.5,
            'step': 0.5,
            'template': 'set1_total_games_under_{n}_vs_over_{n}',
            'home_tpl': 'set1_total_games_under_{n}_odd',
            'away_tpl': 'set1_total_games_over_{n}_odd'
        },
        'set2_total_games_over_under': {
            'type': 'range',
            'start': 6.5,
            'end': 12.5,
            'step': 0.5,
            'template': 'set2_total_games_under_{n}_vs_over_{n}',
            'home_tpl': 'set2_total_games_under_{n}_odd',
            'away_tpl': 'set2_total_games_over_{n}_odd'
        },
        'player1_set1_games_over_under': {
            'type': 'range',
            'start': 2.5,
            'end': 6.5,
            'step': 1.0,
            'template': 'player1_set1_total_games_under_{n}_vs_over_{n}',
            'home_tpl': 'player1_set1_total_games_under_{n}_odd',
            'away_tpl': 'player1_set1_total_games_over_{n}_odd'
        },
        'player2_set1_games_over_under': {
            'type': 'range',
            'start': 2.5,
            'end': 6.5,
            'step': 1.0,
            'template': 'player2_set1_total_games_under_{n}_vs_over_{n}',
            'home_tpl': 'player2_set1_total_games_under_{n}_odd',
            'away_tpl': 'player2_set1_total_games_over_{n}_odd'
        },
        'player1_set2_games_over_under': {
            'type': 'range',
            'start': 2.5,
            'end': 6.5,
            'step': 1.0,
            'template': 'player1_set2_total_games_under_{n}_vs_over_{n}',
            'home_tpl': 'player1_set2_total_games_under_{n}_odd',
            'away_tpl': 'player1_set2_total_games_over_{n}_odd'
        },
        'player2_set2_games_over_under': {
            'type': 'range',
            'start': 2.5,
            'end': 6.5,
            'step': 1.0,
            'template': 'player2_set2_total_games_under_{n}_vs_over_{n}',
            'home_tpl': 'player2_set2_total_games_under_{n}_odd',
            'away_tpl': 'player2_set2_total_games_over_{n}_odd'
        },

        # --- Range Markets - Handicap ---
        'games_handicap': {
            'type': 'range',
            'start': -7.5,
            'end': 7.5,
            'step': 0.5,
            'use_sign': True,
            'template': 'games_handicap_p1_{sign}{n}_vs_p2',
            'home_tpl': 'player1_games_handicap_{sign}{n}_odd',
            'away_tpl': 'player2_games_handicap_{sign}{n}_odd'
        },
    }

    # =================================================================
    # CUSTOMIZATION: Set True/False to enable/disable each market type
    # =================================================================
    ENABLED_MARKETS = {
        'winner': True,
        'set1_winner': True,
        'set2_winner': True,
        'set1_even_odd': True,
        'set2_even_odd': True,
        'total_sets_over_under': True,
        'total_games_over_under': True,
        'player1_total_games_over_under': True,
        'player2_total_games_over_under': True,
        'set1_total_games_over_under': True,
        'set2_total_games_over_under': True,
        'player1_set1_games_over_under': True,
        'player2_set1_games_over_under': True,
        'player1_set2_games_over_under': True,
        'player2_set2_games_over_under': True,
        'games_handicap': True,
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
    print("markets.json generated in compact form for tennis.")