import sys, csv, pathlib, re

# Config
DISALLOWED_PROMO = ["% off", "discount", "sale", "limited offer"]
HEADLINE_MIN, HEADLINE_MAX = 3, 10

def has_promo(text: str) -> bool:
    t = text.lower()
    return any(p in t for p in DISALLOWED_PROMO)

def main():
    root = pathlib.Path(".")
    csv_path = root / "outputs" / "creatives" / "scroll_stoppers.csv"
    img_dir  = root / "outputs" / "creatives" / "images"
    failures = []

    if not csv_path.exists():
        print("FAIL: missing scroll_stoppers.csv"); sys.exit(1)
    if not img_dir.exists():
        print("FAIL: missing images directory"); sys.exit(1)

    with csv_path.open(encoding="utf-8") as f:
        reader = csv.DictReader(f)
        seen_headlines = set()
        for i, row in enumerate(reader, start=2):
            h = (row.get("Headline") or "").strip()
            v = (row.get("Visual") or "").strip()
            a = (row.get("Angle") or "").strip()
            b = (row.get("Blocker") or "").strip()
            cta = re.findall(r"\b(Shop|See|Explore|Discover|Find|Get)\b.*", h + " " + a, flags=re.I)

            # Headline length
            wc = len(h.split())
            if wc < HEADLINE_MIN or wc > HEADLINE_MAX:
                failures.append(f"Row {i}: headline word count {wc} outside [{HEADLINE_MIN},{HEADLINE_MAX}]")

            # Value/Proof presence heuristic
            if not any(k in (h + " " + a).lower() for k in ["proof","review","rating","guarantee","craft","fit","durable","returns","free shipping"]):
                # allow absence if visual carries proof (heuristic weak, flag as warn)
                pass

            # CTA present and non-promo
            if not cta:
                failures.append(f"Row {i}: CTA missing or unclear")
            if has_promo(h) or has_promo(a):
                failures.append(f"Row {i}: disallowed promo CTA")

        # Image presence check (basic)
        if not any(img_dir.glob("*.jpg")) and not any(img_dir.glob("*.jpeg")) and not any(img_dir.glob("*.png")):
            failures.append("No rendered images found")

    if failures:
        print("FAIL ad_readiness_check")
        for f in failures:
            print("-", f)
        sys.exit(1)

    print("PASS ad_readiness_check")

if __name__ == "__main__":
    main()
