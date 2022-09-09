"""
This file is a plug-in to the `analyze-chain` tool.

To use, run with a command-line like:

```
DB=~/.chia/mainnet/db/blockchain_v2_mainnet.sqlite
python -m tools.analyze-chain $DB --start 300000 --end 300100  --call tools.recompress:recompress_block
```

while the current directory is the root `chia-blockchain` folder.

It will iterate over transaction blocks, recompress them and print the new cost ("agony rating")
along with a relative percentage, where 100% is par and smaller is better.
"""

import time
from typing import List

from chia_rs import compression  # type: ignore[attr-defined]

from chia.types.blockchain_format.program import Program, SerializedProgram
from chia.util.full_block_utils import GeneratorBlockInfo

DECOMP_MOD = SerializedProgram.fromhex(
    "ff02ffff01ff02ff05ffff04ff02ffff04ff13ff80808080ffff04ffff01ff02ffff01ff05ffff02"
    "ff3effff04ff02ffff04ff05ff8080808080ffff04ffff01ffffff81ff7fff81df81bfffffff02ff"
    "ff03ffff09ff0bffff01818080ffff01ff04ff80ffff04ff05ff808080ffff01ff02ffff03ffff0a"
    "ff0bff1880ffff01ff02ff1affff04ff02ffff04ffff02ffff03ffff0aff0bff1c80ffff01ff02ff"
    "ff03ffff0aff0bff1480ffff01ff0880ffff01ff04ffff0effff18ffff011fff0b80ffff0cff05ff"
    "80ffff01018080ffff04ffff0cff05ffff010180ff80808080ff0180ffff01ff04ffff18ffff013f"
    "ff0b80ffff04ff05ff80808080ff0180ff80808080ffff01ff04ff0bffff04ff05ff80808080ff01"
    "8080ff0180ff04ffff0cff15ff80ff0980ffff04ffff0cff15ff0980ff808080ffff04ffff04ff05"
    "ff1380ffff04ff2bff808080ffff02ff16ffff04ff02ffff04ff09ffff04ffff02ff3effff04ff02"
    "ffff04ff15ff80808080ff8080808080ff02ffff03ffff09ffff0cff05ff80ffff010180ff1080ff"
    "ff01ff02ff2effff04ff02ffff04ffff02ff3effff04ff02ffff04ffff0cff05ffff010180ff8080"
    "8080ff80808080ffff01ff02ff12ffff04ff02ffff04ffff0cff05ffff010180ffff04ffff0cff05"
    "ff80ffff010180ff808080808080ff0180ff018080ff018080"
)


COST_PER_BYTE = 12000


def recompress_block(
    block: GeneratorBlockInfo, hh: bytes, height: int, generator_blobs: List[bytes], ref_lookup_time: float, flags: int
) -> None:
    MAX_COST = int(1e18)

    assert block.transactions_generator is not None

    original_partial_cost, r_before = DECOMP_MOD.run_with_cost(
        MAX_COST, block.transactions_generator, [generator_blobs]
    )
    original_generator_size = len(bytes(block.transactions_generator))
    original_cost = original_partial_cost + original_generator_size * COST_PER_BYTE

    serialized_decompressed_block = bytes(r_before)

    ttc_start = time.time()
    recompressed_block = compression.create_compressed_generator(serialized_decompressed_block)
    ttc = time.time() - ttc_start

    recompressed_partial_cost, r_after = Program.from_bytes(recompressed_block).run_with_cost(int(1e18), 0)

    assert r_before == r_after

    recompressed_size = len(recompressed_block)
    recompressed_cost = recompressed_partial_cost + recompressed_size * COST_PER_BYTE

    cost_savings_factor = (recompressed_cost / original_cost) * 100

    print(
        f"{height:7d}  Sizes (before, after): {original_generator_size:6d} {recompressed_size:6d}  "
        f"Cost (before, after):  {original_cost:10d}  {recompressed_cost:10d} "
        f"{cost_savings_factor:>6.2f} % (smaller is better)  time to compress: {ttc:2.2f} s"
    )
