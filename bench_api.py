import os
import time
import random
import statistics
import requests



BASE_URL = os.getenv("BASE_URL", "http://localhost:3001")

RIO_LINHAS = [
    "915",
    "371",
    "SVB901",
    "249",
    "232",
    "624",
    "328",
    "606",
    "565",
    "SV624",
    "323",
    "2342",
    "692",
    "634",
    "861",
    "LECD114",
    "932",
    "910",
    "881",
    "550",
    "636",
    "LECD113",
    "390",
    "900",
    "803",
    "862",
    "MANUTENCAO",
    "348",
    "SP328",
    "611",
    "557",
    "863",
    "409",
    "759",
    "368",
    "2336",
    "552",
    "100",
    "133",
    "353",
    "600",
    "306",
    "397",
    "601",
    "878",
    "554",
    "341",
    "613",
    "343",
    "553",
]

ANGRA_LINHAS = [
    "101",
    "102",
    "103",
    "104",
    "105",
    "106",
    "202",
    "203",
    "203Z",
    "204",
    "205",
    "206",
    "221",
    "224",
    "225",
    "227",
    "230",
    "300",
    "306",
    "A207",
    "A208",
    "A220",
    "B101",
    "B106",
    "B203",
    "B206",
    "B207",
    "B208",
    "B220",
    "B221",
    "C01",
    "C01B",
    "C03",
    "C03B",
    "C04",
    "C04B",
    "C05",
    "C05B",
    "C206",
    "T10",
    "T11",
    "T20",
    "T21",
    "T21B",
    "T21C",
]

N = 1000
TIMEOUT = float(os.getenv("TIMEOUT", "10"))
WARMUP = int(os.getenv("WARMUP", "5"))
MODE = os.getenv("MODE", "random").lower()  # random | round_robin


def run_bench(label, endpoint, linhas):
    url = f"{BASE_URL}/{endpoint}"
    linhas_list = list(linhas)
    rr_idx = 0

    def pick_linha():
        nonlocal rr_idx
        if len(linhas_list) == 1:
            return linhas_list[0]
        if MODE == "round_robin":
            linha = linhas_list[rr_idx % len(linhas_list)]
            rr_idx += 1
            return linha
        return random.choice(linhas_list)

    times_ms = []
    times_by_linha = {}
    errors = 0
    errors_by_linha = {}

    for _ in range(WARMUP):
        try:
            linha = pick_linha()
            requests.get(url, params={"linha": linha}, timeout=TIMEOUT)
        except Exception:
            pass

    for _ in range(N):
        linha = pick_linha()
        t0 = time.perf_counter()
        try:
            r = requests.get(url, params={"linha": linha}, timeout=TIMEOUT)
            r.raise_for_status()
        except Exception:
            errors += 1
            errors_by_linha[linha] = errors_by_linha.get(linha, 0) + 1
            continue
        dt_ms = (time.perf_counter() - t0) * 1000
        times_ms.append(dt_ms)
        times_by_linha.setdefault(linha, []).append(dt_ms)

    times_ms_sorted = sorted(times_ms)

    def pct(p: float):
        if not times_ms_sorted:
            return None
        k = int(round((p / 100) * (len(times_ms_sorted) - 1)))
        return times_ms_sorted[k]

    print(f"\n=== BENCHMARK: {label} ===")
    print(f"URL: {url}")
    print(f"Linhas ({len(linhas_list)}): {', '.join(linhas_list)}")
    print(f"Mode: {MODE}")
    print(f"Requests: {N}, ok: {len(times_ms)}, errors: {errors}")
    if times_ms:
        print(f"mean_ms:   {statistics.mean(times_ms):.2f}")
        print(f"median_ms: {statistics.median(times_ms):.2f}")
        print(f"p90_ms:    {pct(90):.2f}")
        print(f"p95_ms:    {pct(95):.2f}")
        print(f"p99_ms:    {pct(99):.2f}")
        print(f"min_ms:    {min(times_ms):.2f}")
        print(f"max_ms:    {max(times_ms):.2f}")

    print("\nPer linha:")
    for linha in sorted(times_by_linha.keys()):
        t = times_by_linha[linha]
        e = errors_by_linha.get(linha, 0)
        if not t:
            print(f"{linha}: ok=0 errors={e}")
            continue
        print(
            f"{linha}: ok={len(t)} errors={e} mean_ms={statistics.mean(t):.2f} "
            f"median_ms={statistics.median(t):.2f} min_ms={min(t):.2f} max_ms={max(t):.2f}"
        )


if __name__ == "__main__":
    run_bench("Rio", "rio_onibus", RIO_LINHAS)
    run_bench("Angra", "angra_onibus", ANGRA_LINHAS)
