# Author: Ridhaant Ajoy Thackur
# AlgoStack v10.2 — resource_plan.py
# CPU/GPU/Memory resource planning for i5-12450H + GTX 1650
# 
# Hardware:
#   CPU: i5-12450H  8 cores / 12 logical  (P-cores 0-7, E-cores 8-11)
#   RAM: 16 GB (≈5 GB free available to AlgoStack)
#   GPU: GTX 1650  4 GB VRAM
#   SSD: NVMe
#
# Process → Core mapping (Windows SetProcessAffinityMask bitmask):
#   Core 0  (P): Algofinal (equity engine)          mask=0x001
#   Core 1  (P): UnifiedDash (dashboard :8055)      mask=0x002
#   Core 2  (P): Scanner1 Narrow 1K×38=38K          mask=0x004
#   Core 3  (P): Scanner2 Dual 16K×38=608K          mask=0x008   ← 16K not 13K
#   Core 4  (P): Scanner3 Wide 32K×38=1,216K        mask=0x010
#   Core 5  (P): CommodityEngine + CommScanners      mask=0x020
#   Core 6  (P): CryptoEngine + CryptoScanners       mask=0x040
#   Core 7  (P): XOptimizer + BestXTrader            mask=0x080
#   Core 8  (E): price_service                       mask=0x100
#   Core 9  (E): autohealer / watchdog               mask=0x200
#   Core 10 (E): EOD writers / async I/O             mask=0x400
#   Core 11 (E): Spare / OS                          mask=0x800
#
# Calculation budget per 1-5 minutes (user spec):
#   Equity:    (1000 + 16000 + 32000) × 38 = 1,862,000
#   Commodity: (1000 + 16000 + 32000) × 5  =   245,000
#   Crypto:    (1000 + 16000 + 32000) × 5  =   245,000
#   TOTAL:     2,352,000 calculations / 1-5 min
#
# At 1 tick/sec × 38 symbols × 49K X-values = 1.86M NumPy ops/sec
# NumPy vectorized over N=49K takes ~0.5ms/symbol → 38×0.5ms = 19ms/tick
# This is well within real-time capability on 12 logical cores.
#
# GPU strategy: GTX 1650 (4GB VRAM) used for Scanner3 (largest, 32K×38=1.2M)
#   CuPy batch: load all 32K x_values once, process each tick as GPU kernel
#   Expected speedup: 15-30× over CPU NumPy for the vectorized math

PROCESS_AFFINITY = {
    "Algofinal":       0x001,   # Core 0
    "UnifiedDash":     0x002,   # Core 1
    "Scanner1":        0x004,   # Core 2
    "Scanner2":        0x008,   # Core 3
    "Scanner3":        0x010,   # Core 4   ← uses GPU if CuPy available
    "CommodityEngine": 0x020,   # Core 5
    "CommScanner1":    0x020,   # Core 5   (lightweight, share with engine)
    "CommScanner2":    0x020,   # Core 5
    "CommScanner3":    0x020,   # Core 5
    "CryptoEngine":    0x040,   # Core 6
    "CryptoScanner1":  0x040,   # Core 6
    "CryptoScanner2":  0x040,   # Core 6
    "CryptoScanner3":  0x040,   # Core 6
    "XOptimizer":      0x080,   # Core 7
    "BestXTrader":     0x080,   # Core 7
    "price_service":   0x100,   # Core 8
}

# Memory allocation guide (with 5GB free):
#   Scanner1 (38 × 1K × float64):   38×1K×8B  = 0.30 MB
#   Scanner2 (38 × 16K × float64):  38×16K×8B = 4.87 MB
#   Scanner3 (38 × 32K × float64):  38×32K×8B = 9.75 MB  (GPU: 9.75 MB VRAM)
#   CommScanners (5 × 49K × float64): 1.96 MB total
#   CryptoScanners (5 × 49K × float64): 1.96 MB total
#   ZMQ queues + trade logs:          ~50 MB
#   TOTAL AlgoStack RAM:              ~70 MB (well within 5GB free)
