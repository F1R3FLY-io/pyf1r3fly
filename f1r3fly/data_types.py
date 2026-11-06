from dataclasses import dataclass
from typing import List

from . import token_amount
from .meta import from_pb


@from_pb
@dataclass
class DeployInfo:
    deployer: str
    term: str
    timestamp: int
    sig: str
    sigAlgorithm: str
    phloPrice: int
    phloLimit: int
    validAfterBlockNumber: int
    cost: int
    errored: bool
    systemDeployError: str

    def __post_init__(self) -> None:
        if isinstance(self.phloPrice, str):
            self.phloPrice = token_amount.parse(self.phloPrice)


@from_pb
@dataclass
class BondInfo:
    validator: str
    stake: int

    def __post_init__(self) -> None:
        if isinstance(self.stake, str):
            self.stake = token_amount.parse(self.stake)


@from_pb
@dataclass
class LightBlockInfo:
    # BlockMessageProto
    blockHash: str
    sender: str
    seqNum: int
    sig: str
    sigAlgorithm: str
    shardId: str
    extraBytes: bytes

    # HeaderProto
    version: int
    timestamp: int
    headerExtraBytes: bytes
    parentsHashList: List[str]

    # BodyProto
    blockNumber: int
    preStateHash: str
    postStateHash: str
    bodyExtraBytes: bytes
    BondInfo: List[BondInfo]

    # extra
    blockSize: str
    deployCount: int
    faultTolerance: float


@from_pb
@dataclass
class BlockInfo:
    blockInfo: LightBlockInfo
    deploys: List[DeployInfo]
