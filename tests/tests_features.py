import pandas as pd, pytest


def compute_features(df):
    df = df.copy()
    df['trip_minutes']       = df['trip_seconds'] / 60.0
    df['fare_per_mile']      = df['fare'] / df['trip_miles'].replace(0, float('nan'))
    df['fare_per_minute']    = df['fare'] / df['trip_minutes'].replace(0, float('nan'))
    df['is_weekend']         = df['pickup_dow'].isin([0,6]).astype(int)
    df['is_late_night']      = df['pickup_hour'].isin([22,23,0,1,2,3]).astype(int)
    df['pickup_is_airport']  = df['pickup_area'].isin(['76','56']).astype(int)
    df['dropoff_is_airport'] = df['dropoff_area'].isin(['76','56']).astype(int)
    df['tipped']             = (df['tips'] > 0).astype(int)
    return df


SAMPLE = pd.DataFrame([{'trip_seconds':120,'trip_miles':3.5,'fare':14.0,'tips':2.5,
                         'extras':0.0,'pickup_dow':3,'pickup_hour':14,
                         'pickup_area':'8','dropoff_area':'32'}])


def test_fare_per_mile():
    df = compute_features(SAMPLE)
    assert abs(df['fare_per_mile'].iloc[0] - 14.0/3.5) < 0.01


def test_trip_minutes():
    df = compute_features(SAMPLE)
    assert df['trip_minutes'].iloc[0] == pytest.approx(2.0)


def test_not_weekend_wednesday():
    df = compute_features(SAMPLE)   # dow=3 is Wednesday
    assert df['is_weekend'].iloc[0] == 0


def test_not_late_night_afternoon():
    df = compute_features(SAMPLE)   # hour=14 is 2pm
    assert df['is_late_night'].iloc[0] == 0


def test_airport_ohare_flag():
    s = SAMPLE.copy(); s['pickup_area'] = '76'
    df = compute_features(s)
    assert df['pickup_is_airport'].iloc[0] == 1


def test_not_airport_normal_area():
    df = compute_features(SAMPLE)
    assert df['pickup_is_airport'].iloc[0] == 0


def test_tipped_positive():
    df = compute_features(SAMPLE)
    assert df['tipped'].iloc[0] == 1


def test_not_tipped_zero():
    s = SAMPLE.copy(); s['tips'] = 0
    df = compute_features(s)
    assert df['tipped'].iloc[0] == 0


def test_late_night_midnight():
    s = SAMPLE.copy(); s['pickup_hour'] = 0
    df = compute_features(s)
    assert df['is_late_night'].iloc[0] == 1
