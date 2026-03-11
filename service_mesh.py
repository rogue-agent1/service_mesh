#!/usr/bin/env python3
"""Service mesh — sidecar proxy simulation with routing, retries, circuit breaking.

One file. Zero deps. Does one thing well.

Simulates Envoy/Istio-style service mesh: service discovery, load balancing,
circuit breaking, retries with backoff, and distributed tracing.
"""
import random, time, sys
from collections import defaultdict
from enum import Enum

class CircuitState(Enum):
    CLOSED = "closed"       # Normal operation
    OPEN = "open"           # Failing, reject requests
    HALF_OPEN = "half-open" # Testing recovery

class CircuitBreaker:
    def __init__(self, threshold=5, timeout=10.0, half_open_max=3):
        self.threshold = threshold
        self.timeout = timeout
        self.half_open_max = half_open_max
        self.state = CircuitState.CLOSED
        self.failures = 0
        self.successes = 0
        self.last_failure = 0
        self.half_open_calls = 0

    def allow(self):
        if self.state == CircuitState.CLOSED:
            return True
        if self.state == CircuitState.OPEN:
            if time.time() - self.last_failure > self.timeout:
                self.state = CircuitState.HALF_OPEN
                self.half_open_calls = 0
                return True
            return False
        # Half-open
        return self.half_open_calls < self.half_open_max

    def record_success(self):
        if self.state == CircuitState.HALF_OPEN:
            self.successes += 1
            self.half_open_calls += 1
            if self.successes >= self.half_open_max:
                self.state = CircuitState.CLOSED
                self.failures = 0
                self.successes = 0
        self.failures = max(0, self.failures - 1)

    def record_failure(self):
        self.failures += 1
        self.last_failure = time.time()
        if self.state == CircuitState.HALF_OPEN:
            self.state = CircuitState.OPEN
        elif self.failures >= self.threshold:
            self.state = CircuitState.OPEN

class Service:
    def __init__(self, name, fail_rate=0.0, latency=10):
        self.name = name
        self.fail_rate = fail_rate
        self.latency = latency
        self.requests = 0

    def handle(self):
        self.requests += 1
        if random.random() < self.fail_rate:
            raise Exception(f"{self.name}: 503 Service Unavailable")
        return {"status": 200, "service": self.name, "latency": self.latency}

class SidecarProxy:
    """Envoy-style sidecar proxy."""
    def __init__(self, service_name):
        self.service_name = service_name
        self.upstreams = {}      # service_name -> [Service]
        self.breakers = {}       # service_name -> CircuitBreaker
        self.retry_policy = {"max_retries": 3, "backoff_ms": 100}
        self.traces = []

    def register_upstream(self, name, instances):
        self.upstreams[name] = instances
        self.breakers[name] = CircuitBreaker()

    def call(self, target, trace_id=None):
        """Route request to target service with retries and circuit breaking."""
        trace_id = trace_id or f"trace-{random.randint(1000,9999)}"
        span = {"trace_id": trace_id, "from": self.service_name, "to": target,
                "start": time.time(), "attempts": 0, "status": None}

        cb = self.breakers.get(target)
        if cb and not cb.allow():
            span["status"] = "circuit_open"
            self.traces.append(span)
            return {"status": 503, "error": "circuit breaker open"}

        instances = self.upstreams.get(target, [])
        if not instances:
            span["status"] = "no_instances"
            self.traces.append(span)
            return {"status": 503, "error": "no upstream instances"}

        last_error = None
        for attempt in range(self.retry_policy["max_retries"] + 1):
            span["attempts"] += 1
            instance = random.choice(instances)
            try:
                result = instance.handle()
                if cb: cb.record_success()
                span["status"] = "ok"
                span["instance"] = instance.name
                self.traces.append(span)
                return result
            except Exception as e:
                last_error = e
                if cb: cb.record_failure()

        span["status"] = "failed"
        span["error"] = str(last_error)
        self.traces.append(span)
        return {"status": 503, "error": str(last_error)}

def main():
    random.seed(42)
    print("=== Service Mesh Simulation ===\n")

    # Create services
    api_instances = [Service(f"api-{i}", fail_rate=0.1) for i in range(3)]
    db_instances = [Service(f"db-{i}", fail_rate=0.05, latency=20) for i in range(2)]
    cache_instances = [Service("cache-0", fail_rate=0.02, latency=2)]

    # Create sidecar for gateway
    gateway = SidecarProxy("gateway")
    gateway.register_upstream("api", api_instances)
    gateway.register_upstream("db", db_instances)
    gateway.register_upstream("cache", cache_instances)

    # Simulate traffic
    results = defaultdict(int)
    for i in range(100):
        for target in ["api", "cache", "api", "db"]:
            r = gateway.call(target, f"trace-{i}")
            results[r["status"]] += 1

    print("Request results:")
    for status, count in sorted(results.items()):
        print(f"  {status}: {count}")

    print(f"\nCircuit breaker states:")
    for name, cb in gateway.breakers.items():
        print(f"  {name}: {cb.state.value} (failures={cb.failures})")

    print(f"\nBackend load:")
    for instances in [api_instances, db_instances, cache_instances]:
        for s in instances:
            print(f"  {s.name}: {s.requests} requests")

    print(f"\nTraces: {len(gateway.traces)} spans")
    retried = sum(1 for t in gateway.traces if t["attempts"] > 1)
    print(f"  Retried: {retried} requests")

if __name__ == "__main__":
    main()
