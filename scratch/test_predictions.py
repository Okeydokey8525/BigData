import sys
import os

if sys.stdout and hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')
if sys.stderr and hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(encoding='utf-8')

sys.path.append('Web_Air')
from core.model_loader import model_manager
from core.predictor import build_13_features, predict_single_model

model_manager.load_all()

test_cases = [
    ('Chuyến bay sáng sớm JFK->LAX (DL)', 'DL', 'JFK', 'LAX', '2024-07-15', '06:00', 360, 2475),
    ('Chuyến bay tối muộn ORD->ATL (AA)', 'AA', 'ORD', 'ATL', '2024-08-20', '21:45', 130, 606),
    ('Chuyến bay bão mùa đông ORD->LGA (UA)', 'UA', 'ORD', 'LGA', '2024-12-24', '20:30', 140, 733),
    ('Chuyến bay đêm DFW->MIA (WN)', 'WN', 'DFW', 'MIA', '2024-09-18', '22:15', 170, 1121),
    ('Chuyến bay chiều tối SFO->JFK (B6)', 'B6', 'SFO', 'JFK', '2024-06-15', '19:30', 330, 2586),
    ('Chuyến bay EWR->ORD giờ cao điểm (UA)', 'UA', 'EWR', 'ORD', '2024-07-10', '18:15', 155, 719),
    ('Chuyến bay F9 (Frontier) tối muộn DEN->LAS', 'F9', 'DEN', 'LAS', '2024-11-20', '23:10', 115, 628),
    ('Chuyến bay NK (Spirit) tối muộn ATL->FLL', 'NK', 'ATL', 'FLL', '2024-12-15', '22:45', 110, 581),
    ('Chuyến bay G4 (Allegiant) tối muộn PIE->CVG', 'G4', 'PIE', 'CVG', '2024-12-22', '21:00', 130, 800),
    ('Chuyến bay B6 (JetBlue) đêm muộn JFK->BOS', 'B6', 'JFK', 'BOS', '2024-12-31', '23:45', 75, 187),
]

for name, carrier, orig, dest, dt, t_str, elap, dist in test_cases:
    print(f"\n=== {name} ===")
    try:
        feats = build_13_features(carrier, orig, dest, dt, t_str, elap, dist)
        for mid in ['decision_tree', 'random_forest_cpu', 'catboost', 'lightgbm', 'xgboost']:
            res = predict_single_model(mid, feats)
            p_code = res['predicted_code']
            p_name = res['predicted_name']
            conf = res['confidence_percent']
            probs = {p['name']: p['percent'] for p in res['probabilities'] if p['percent'] > 5.0}
            print(f"  {mid:<18}: code={p_code} ({p_name}) | Top probs: {probs}")
    except Exception as e:
        print(f"  Error: {e}")
