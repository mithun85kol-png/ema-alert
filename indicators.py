def detect_candle_pattern(row):
    """
    Classifies a single candle as one of: Marubozu (Bullish/Bearish),
    Hammer, Inverted Hammer, Shooting Star, Doji, or Normal.
    Returns a short label string.
    """
    high, low, open_, close = row["high"], row["low"], row["open"], row["close"]
    candle_range = high - low
    if candle_range <= 0:
        return "N/A"

    body = abs(close - open_)
    body_ratio = body / candle_range
    upper_wick = high - max(open_, close)
    lower_wick = min(open_, close) - low

    # Doji: very small body relative to range
    if body_ratio < 0.1:
        return "Doji"

    # Marubozu: body takes up almost the whole range (tiny/no wicks)
    if body_ratio > 0.9:
        return "Bullish Marubozu" if close > open_ else "Bearish Marubozu"

    # Hammer: small body near top, long lower wick, little/no upper wick
    if lower_wick >= 2 * body and upper_wick <= body * 0.3:
        return "Hammer"

    # Shooting Star / Inverted Hammer: small body near bottom, long upper wick
    if upper_wick >= 2 * body and lower_wick <= body * 0.3:
        return "Shooting Star" if close < open_ else "Inverted Hammer"

    return "Normal"
