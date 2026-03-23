# Author: Ridhaant Ajoy Thackur
# AlgoStack v10.2 — process_affinity.py
# Sets CPU core affinity for all AlgoStack processes on Windows (i5-12450H)
"""
Call pin_process(name) at the top of each main() to bind the process to its
designated core(s). This prevents OS task switching between scanner processes
and ensures each scanner gets dedicated CPU time.

On i5-12450H (12 logical processors):
  P-cores (0-7):  high-performance, used for trading engines + scanners
  E-cores (8-11): efficiency, used for I/O processes (price_service, watchdog)
"""
from __future__ import annotations
import logging
import os
import sys

log = logging.getLogger("affinity")

# Windows CPU affinity bitmask per process name
# Each bit = one logical processor (bit 0 = CPU0, bit 1 = CPU1, etc.)
AFFINITY_MAP = {
    # P-cores (0–7): trading-critical
    "Algofinal":       0x003,   # Cores 0+1 (needs most for equity engine)
    "UnifiedDash":     0x002,   # Core 1    (dashboard rendering)
    "Scanner1":        0x004,   # Core 2    (narrow 1K×38 = fast)
    "Scanner2":        0x008,   # Core 3    (dual 16K×38 = medium)
    "Scanner3":        0x010,   # Core 4    (wide 32K×38 = heaviest)
    "XOptimizer":      0x080,   # Core 7    (aggregation)
    "BestXTrader":     0x080,   # Core 7    (paper trading)
    # Commodity (share core 5)
    "CommodityEngine": 0x060,   # Cores 5+6
    "CommScanner1":    0x020,   # Core 5
    "CommScanner2":    0x020,   # Core 5
    "CommScanner3":    0x020,   # Core 5
    # Crypto (share core 6)
    "CryptoEngine":    0x040,   # Core 6
    "CryptoScanner1":  0x040,   # Core 6
    "CryptoScanner2":  0x040,   # Core 6
    "CryptoScanner3":  0x040,   # Core 6
    # E-cores (8–11): I/O and monitoring
    "price_service":   0x100,   # Core 8    (yfinance REST calls)
    "autohealer":      0x200,   # Core 9    (watchdog)
    "news_dashboard":  0x400,   # Core 10
}

# Process priority mapping
PRIORITY_MAP = {
    "Scanner1":        "ABOVE_NORMAL",
    "Scanner2":        "ABOVE_NORMAL",
    "Scanner3":        "HIGH",           # GPU-accelerated, needs responsiveness
    "CryptoScanner1":  "ABOVE_NORMAL",
    "CryptoScanner2":  "ABOVE_NORMAL",
    "CryptoScanner3":  "ABOVE_NORMAL",
    "CommScanner1":    "NORMAL",
    "CommScanner2":    "NORMAL",
    "CommScanner3":    "NORMAL",
    "Algofinal":       "HIGH",
    "CryptoEngine":    "ABOVE_NORMAL",
    "CommodityEngine": "NORMAL",
    "price_service":   "NORMAL",
    "UnifiedDash":     "NORMAL",
    "autohealer":      "NORMAL",
}

# Windows PROCESS_PRIORITY_CLASS values
_WIN_PRIORITY = {
    "IDLE":         0x00000040,
    "BELOW_NORMAL": 0x00004000,
    "NORMAL":       0x00000020,
    "ABOVE_NORMAL": 0x00008000,
    "HIGH":         0x00000080,
    "REALTIME":     0x00000100,
}


def pin_process(name: str, *, verbose: bool = True) -> bool:
    """
    Pin current process to its designated CPU cores and set priority.
    Call once at the start of each process's main().
    
    Returns True on success, False if affinity setting not supported.
    """
    pid = os.getpid()
    mask = AFFINITY_MAP.get(name)
    priority = PRIORITY_MAP.get(name, "NORMAL")
    success = False

    if mask is not None:
        if sys.platform == "win32":
            try:
                import ctypes
                kernel32 = ctypes.windll.kernel32
                handle = kernel32.OpenProcess(0x0200 | 0x0400, False, pid)
                if handle:
                    ok = kernel32.SetProcessAffinityMask(handle, mask)
                    kernel32.CloseHandle(handle)
                    if ok and verbose:
                        cores = [i for i in range(16) if mask & (1 << i)]
                        log.info("[%s] CPU affinity → cores %s", name, cores)
                    success = bool(ok)
            except Exception as exc:
                log.debug("[%s] SetProcessAffinityMask failed: %s", name, exc)
        elif sys.platform.startswith("linux"):
            try:
                cores = [i for i in range(16) if mask & (1 << i)]
                os.sched_setaffinity(pid, set(cores))
                if verbose:
                    log.info("[%s] CPU affinity → cores %s", name, cores)
                success = True
            except Exception as exc:
                log.debug("[%s] sched_setaffinity failed: %s", name, exc)

    # Set process priority (Windows)
    if sys.platform == "win32":
        try:
            import ctypes
            pclass = _WIN_PRIORITY.get(priority, _WIN_PRIORITY["NORMAL"])
            ctypes.windll.kernel32.SetPriorityClass(
                ctypes.windll.kernel32.GetCurrentProcess(), pclass
            )
            if verbose:
                log.info("[%s] Priority → %s", name, priority)
        except Exception as exc:
            log.debug("[%s] SetPriorityClass failed: %s", name, exc)

    return success


def get_cpu_count() -> int:
    """Return number of logical processors available."""
    try:
        return len(os.sched_getaffinity(0))
    except AttributeError:
        return os.cpu_count() or 12


def get_optimal_workers(process_name: str, task_type: str = "compute") -> int:
    """
    Return optimal worker thread/process count for a given process.
    
    task_type: "compute" | "io" | "mixed"
    """
    total = get_cpu_count()
    mask = AFFINITY_MAP.get(process_name, 0xFFF)
    allocated = bin(mask).count("1")

    if task_type == "compute":
        # Use all allocated cores for compute
        return max(1, allocated)
    elif task_type == "io":
        # I/O is thread-based, can use 2-4× cores
        return min(allocated * 3, 8)
    else:  # mixed
        return max(1, allocated)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    print(f"CPU count: {get_cpu_count()}")
    for name in AFFINITY_MAP:
        cores = [i for i in range(16) if AFFINITY_MAP[name] & (1<<i)]
        workers_c = get_optimal_workers(name, "compute")
        workers_i = get_optimal_workers(name, "io")
        print(f"  {name:<20}: cores={cores}  compute_workers={workers_c}  io_workers={workers_i}")
