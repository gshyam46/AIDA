"""Small sequential local HTTP benchmark; not a load or capacity test."""
import argparse
import json
import statistics
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

QUERIES = ["Count all orders", "Total revenue", "Revenue by region", "Monthly revenue trend", "Top 3 categories by revenue", "Revenue in West last month"]

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--rounds", type=int, default=10)
    args = parser.parse_args()
    if args.rounds < 1:
        parser.error("rounds must be at least 1")
    path = Path(__file__).resolve().parents[1] / "artifacts" / "benchmark.json"
    path.parent.mkdir(exist_ok=True)
    durations, hits, model_calls, semantic_hits, tokens = [], 0, 0, 0, 0
    inference_ms, reused_ms = [], []
    for _ in range(args.rounds):
        for question in QUERIES:
            started = time.perf_counter()
            request = urllib.request.Request(args.base_url.rstrip("/") + "/api/v1/query",
                data=json.dumps({"question": question}).encode(), headers={"Content-Type": "application/json"})
            with urllib.request.urlopen(request, timeout=120) as response:
                result = json.load(response)
            durations.append((time.perf_counter() - started) * 1000)
            if not result["success"]:
                path.write_text(json.dumps({"status": "failed", "timestamp": datetime.now(timezone.utc).isoformat(),
                    "question": question, "response": result, "completed_requests": len(durations) - 1}, indent=2), encoding="utf-8")
                raise SystemExit(f"Benchmark query failed: {question}")
            hits += int(result["meta"].get("cache_hit", False))
            model_calls += result["meta"]["model_calls"]
            semantic_hits += int(result["meta"].get("interpretation_cache_hit", False))
            tokens += result["meta"].get("total_tokens") or 0
            (inference_ms if result["meta"]["model_calls"] else reused_ms).append(durations[-1])
    result = {"status": "passed", "timestamp": datetime.now(timezone.utc).isoformat(), "base_url": args.base_url,
        "requests": len(durations), "successes": len(durations), "model_calls": model_calls,
        "cache_hits": hits, "p50_http_ms": round(statistics.median(durations), 2),
        "semantic_cache_hits": semantic_hits, "actual_tokens": tokens,
        "p50_inference_request_ms": round(statistics.median(inference_ms), 2) if inference_ms else None,
        "p50_reused_request_ms": round(statistics.median(reused_ms), 2) if reused_ms else None,
        "p95_http_ms": round(sorted(durations)[int((len(durations) - 1) * .95)], 2),
        "max_http_ms": round(max(durations), 2),
        "scope": "Sequential requests to a local server; includes warmed cache. Not a capacity benchmark."}
    path.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps(result, indent=2))

if __name__ == "__main__":
    main()
