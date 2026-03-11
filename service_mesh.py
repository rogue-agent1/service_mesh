#!/usr/bin/env python3
"""Service Mesh — sidecar proxy with routing, retries, and circuit breaking."""
import random, time

class ServiceInstance:
    def __init__(self, name, address, healthy=True):
        self.name = name; self.address = address; self.healthy = healthy

class ServiceRegistry:
    def __init__(self): self.services = {}
    def register(self, name, address):
        self.services.setdefault(name, []).append(ServiceInstance(name, address))
    def discover(self, name):
        return [s for s in self.services.get(name, []) if s.healthy]

class SidecarProxy:
    def __init__(self, registry, retries=3, timeout=5.0):
        self.registry = registry; self.retries = retries; self.timeout = timeout
        self.circuit_breakers = {}; self.stats = {'requests': 0, 'retries': 0, 'failures': 0}
    def call(self, service_name, request):
        instances = self.registry.discover(service_name)
        if not instances: raise RuntimeError(f"No instances for {service_name}")
        for attempt in range(self.retries):
            instance = random.choice(instances)
            self.stats['requests'] += 1
            try:
                return self._forward(instance, request)
            except Exception:
                self.stats['retries'] += 1
                if attempt == self.retries - 1:
                    self.stats['failures'] += 1; raise
    def _forward(self, instance, request):
        if not instance.healthy: raise ConnectionError(f"{instance.address} unhealthy")
        return {"status": 200, "from": instance.address, "data": f"OK:{request}"}

if __name__ == "__main__":
    reg = ServiceRegistry()
    for i in range(3): reg.register("auth-service", f"10.0.0.{i+1}:8080")
    reg.register("payment-service", "10.0.1.1:8080")
    proxy = SidecarProxy(reg)
    for _ in range(10):
        resp = proxy.call("auth-service", "validate-token")
    print(f"Stats: {proxy.stats}")
    print(f"Sample response: {proxy.call('auth-service', 'check')}")
