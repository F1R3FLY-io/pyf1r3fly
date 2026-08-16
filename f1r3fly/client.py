import logging
import re
from types import TracebackType
from typing import Any, Iterable, List, Optional, Tuple, Type, TypeVar, Union

import grpc

from .crypto import PrivateKey, PublicKey
from .par import par_value
from .param import Params
from .pb.CasperMessage_pb2 import DeployDataProto
from .pb.DeployServiceCommon_pb2 import (
    BlockInfo, BlockQuery, BlocksQuery, BlocksQueryByHeight, BondStatusQuery,
    ContinuationAtNameQuery, DataAtNameByBlockQuery, DataAtNameQuery,
    DeployFinalizationStatusInfo, DeployFinalizationStatusQuery,
    ExploratoryDeployQuery, FindDeployQuery, IsFinalizedQuery,
    LastFinalizedBlockQuery, LightBlockInfo, PrivateNamePreviewQuery,
    ReportQuery, SingleReport, Status, VisualizeDagQuery,
)
from .pb.DeployServiceV1_pb2 import (
    BlockInfoResponse, BlockResponse, ContinuationAtNameResponse,
    DeployResponse, EventInfoResponse, ExploratoryDeployResponse,
    PrivateNamePreviewResponse, RhoDataPayload, VisualizeBlocksResponse,
)
from .pb.DeployServiceV1_pb2_grpc import DeployServiceStub
from .pb.ProposeServiceCommon_pb2 import ProposeQuery
from .pb.ProposeServiceV1_pb2 import ProposeResponse
from .pb.ProposeServiceV1_pb2_grpc import ProposeServiceStub
from .pb.RhoTypes_pb2 import CostSignature, Expr, GDeployId, GUnforgeable, Par
from .report import DeployWithTransaction, Report, Transaction
from .util import create_deploy_data

GRPC_Response_T = Union[ProposeResponse,
                        DeployResponse,
                        BlockResponse,
                        BlockInfoResponse,
                        ExploratoryDeployResponse,
                        VisualizeBlocksResponse]

GRPC_StreamResponse_T = Union[BlockInfoResponse, VisualizeBlocksResponse]
T = TypeVar("T")

propose_result_match = re.compile(r'Success! Block (?P<block_hash>[0-9a-f]+) created and added.')


class F1r3flyClientException(Exception):

    def __init__(self, message: str) -> None:
        super().__init__(message)


class DataQueries:

    @staticmethod
    def public_names(names: List[str]) -> Par:
        exprs = [Expr(g_string=n) for n in names]
        return Par(exprs=exprs)

    @staticmethod
    def deploy_id(deploy_id: str) -> Par:
        g_deploy_id = GDeployId(sig=bytes.fromhex(deploy_id))
        g_unforgeable = GUnforgeable(g_deploy_id_body=g_deploy_id)
        return Par(unforgeables=[g_unforgeable])


class F1r3flyClient:

    def __init__(self, host: str, port: int, grpc_options: Optional[Tuple[Tuple[str, Union[str, int]], ...]] = None,
                 compress: bool = False, timeout: float = 30.0):
        compression = grpc.Compression.Gzip if compress else None
        self.channel = grpc.insecure_channel("{}:{}".format(host, port), grpc_options, compression)
        self._deploy_stub = DeployServiceStub(self.channel)
        self.timeout = timeout
        self.param: Optional[Params] = None

    def close(self) -> None:
        self.channel.close()

    def __enter__(self) -> 'F1r3flyClient':
        return self

    def __exit__(self, exc_type: Optional[Type[BaseException]],
                 exc_val: Optional[BaseException],
                 exc_tb: Optional[TracebackType]) -> None:
        self.close()

    def install_param(self, param: Params) -> None:
        self.param = param

    def _check_response(self, response: GRPC_Response_T) -> None:
        logging.debug('gRPC response: %s', str(response))
        if response.WhichOneof("message") == 'error':
            raise F1r3flyClientException('\n'.join(response.error.messages))

    def _handle_stream(self, response: Iterable[GRPC_StreamResponse_T]) -> List[GRPC_StreamResponse_T]:
        result = []
        for resp in response:
            self._check_response(resp)
            result.append(resp)
        return result

    def deploy_with_vabn_filled(
            self,
            key: PrivateKey,
            term: str,
            timestamp_millis: int = -1,
            shard_id: str = '',
            expiration_timestamp: int = 0,
            authority_presentations: Iterable[CostSignature] = (),
    ) -> str:
        latest_blocks = self.show_blocks(1)
        # when the genesis block is not ready, it would be empty in show_blocks
        # it could return more than 1 block when there are multiple blocks at the same height
        assert len(latest_blocks) >= 1, "No latest block found"
        latest_block = latest_blocks[0]
        latest_block_num = latest_block.blockNumber
        return self.deploy(
            key,
            term,
            latest_block_num,
            timestamp_millis,
            shard_id,
            expiration_timestamp,
            authority_presentations,
        )

    def exploratory_deploy(self, term: str, blockHash: str, usePreStateHash: bool = False) -> List[Par]:
        exploratory_query = ExploratoryDeployQuery(term=term, blockHash=blockHash, usePreStateHash=usePreStateHash)
        response = self._deploy_stub.exploratoryDeploy(exploratory_query, timeout=self.timeout)
        self._check_response(response)
        return list(response.result.postBlockData)

    def read_channel(self, channel: str, block_hash: str = "") -> Any:
        """Peek the current value on a named channel via a non-consuming
        exploratory read (``<<-``), returning the auto-typed Python value
        (``dict`` for a Map, ``int`` for an Int, ``str`` for a String, ...),
        or ``None`` when the channel holds no value.

        Read-only: no block created, no phlo consumed. ``channel`` is the bare
        name used as ``@"<channel>"`` in the contract. Reading against the
        latest state by default; pass ``block_hash`` to read at a specific
        block (e.g. a finalized hash).
        """
        term = f'new return in {{ for (@v <<- @"{channel}") {{ return!(v) }} }}'
        results = self.exploratory_deploy(term, block_hash)
        if not results:
            return None
        return par_value(results[0])

    def deploy(
            self,
            key: PrivateKey,
            term: str,
            valid_after_block_no: int = -1,
            timestamp_millis: int = -1,
            shard_id: str = '',
            expiration_timestamp: int = 0,
            authority_presentations: Iterable[CostSignature] = (),
    ) -> str:
        deploy_data = create_deploy_data(
            key,
            term,
            valid_after_block_no,
            timestamp_millis,
            shard_id,
            expiration_timestamp,
            authority_presentations,
        )
        return self.send_deploy(deploy_data)

    def send_deploy(self, deploy: DeployDataProto) -> str:
        response = self._deploy_stub.doDeploy(deploy, timeout=self.timeout)
        self._check_response(response)
        # sig of deploy data is deployId
        return deploy.sig.hex()

    def show_block(self, block_hash: str) -> BlockInfo:
        block_query = BlockQuery(hash=block_hash)
        response = self._deploy_stub.getBlock(block_query, timeout=self.timeout)
        self._check_response(response)
        return response.blockInfo

    def show_blocks(self, depth: int = 1) -> List[LightBlockInfo]:
        blocks_query = BlocksQuery(depth=depth)
        response = self._deploy_stub.getBlocks(blocks_query, timeout=self.timeout)
        result = self._handle_stream(response)
        return list(map(lambda x: x.blockInfo, result))  # type: ignore

    def find_deploy(self, deploy_id: str) -> LightBlockInfo:
        find_deploy_query = FindDeployQuery(deployId=bytes.fromhex(deploy_id))
        response = self._deploy_stub.findDeploy(find_deploy_query, timeout=self.timeout)
        self._check_response(response)
        return response.blockInfo

    def last_finalized_block(self) -> BlockInfo:
        last_finalized_query = LastFinalizedBlockQuery()
        response = self._deploy_stub.lastFinalizedBlock(last_finalized_query, timeout=self.timeout)
        self._check_response(response)
        return response.blockInfo

    def is_finalized(self, block_hash: str) -> bool:
        is_finalized_query = IsFinalizedQuery(hash=block_hash)
        response = self._deploy_stub.isFinalized(is_finalized_query, timeout=self.timeout)
        self._check_response(response)
        return response.isFinalized

    def deploy_finalization_status(self, deploy_sig_hex: str) -> DeployFinalizationStatusInfo:
        """Query canonical-state finalization status for a deploy by its signature.

        Prefer this over ``is_finalized(block_hash)`` for deploy tracking.
        ``is_finalized`` can return True for a block whose deploy effects were
        dropped by multi-parent merge rejection; this API reports the deploy's
        actual state in canonical state.

        Returns a ``DeployFinalizationStatusInfo`` with:
          - ``state``: ``DeployFinalizationStateProto`` enum
            (``DEPLOY_STATE_FINALIZED`` / ``FAILED`` / ``PENDING`` / ``EXPIRED``)
          - ``rejectionCount``: number of times this deploy was rejected during
            merges (monotonically increases, carries through to terminal states)
          - ``latestBlockHash``: bytes of the most recent canonical block
            containing this deploy sig; empty/absent when the deploy has
            never been included.
        """
        query = DeployFinalizationStatusQuery(deploySig=bytes.fromhex(deploy_sig_hex))
        response = self._deploy_stub.deployFinalizationStatus(query, timeout=self.timeout)
        self._check_response(response)
        return response.status

    def status(self) -> Status:
        """Get node status. Returns Status proto object."""
        from google.protobuf.empty_pb2 import Empty
        response = self._deploy_stub.status(Empty(), timeout=self.timeout)
        self._check_response(response)
        return response.status

    def bond_status(self, public_key_hex: str) -> bool:
        """Check if a public key is bonded. Returns bool."""
        public_key_bytes = bytes.fromhex(public_key_hex)
        query = BondStatusQuery(publicKey=public_key_bytes)
        response = self._deploy_stub.bondStatus(query, timeout=self.timeout)
        self._check_response(response)
        return response.isBonded

    def propose(self, is_async: bool = False) -> str:
        stub = ProposeServiceStub(self.channel)
        response: ProposeResponse = stub.propose(ProposeQuery(isAsync=is_async), timeout=self.timeout)
        self._check_response(response)
        match_result = propose_result_match.match(response.result)
        assert match_result is not None
        return match_result.group("block_hash")

    def get_data_at_par(self, par: Par, block_hash: str, use_pre_state_hash: bool = False) -> Optional[RhoDataPayload]:
        query = DataAtNameByBlockQuery(par=par, blockHash=block_hash, usePreStateHash=use_pre_state_hash)
        response = self._deploy_stub.getDataAtName(query, timeout=self.timeout)
        if response.WhichOneof("message") == 'error':
            error_msg = '\n'.join(response.error.messages)
            if "No data found" in error_msg:
                return None
            raise F1r3flyClientException(error_msg)
        wrapped = response.payload
        return RhoDataPayload.FromString(wrapped.SerializeToString())

    def get_data_at_deploy_id(self, deploy_id: str, block_hash: str = "") -> Optional[RhoDataPayload]:
        """Get data sent to a deploy's deployId channel.

        Requires block_hash — queries against a specific block's post-state
        via getDataAtName gRPC method.
        """
        if not block_hash:
            raise F1r3flyClientException("block_hash is required for get_data_at_deploy_id")
        par = DataQueries.deploy_id(deploy_id)
        return self.get_data_at_par(par, block_hash)

    def show_main_chain(self, depth: int = 1) -> List[LightBlockInfo]:
        """Get blocks on the main chain. Streaming. Returns LightBlockInfo list."""
        blocks_query = BlocksQuery(depth=depth)
        response = self._deploy_stub.showMainChain(blocks_query, timeout=self.timeout)
        result = self._handle_stream(response)
        return list(map(lambda x: x.blockInfo, result))  # type: ignore

    def get_blocks_by_heights(self, start_block_number: int, end_block_number: int) -> List[LightBlockInfo]:
        query = BlocksQueryByHeight(startBlockNumber=start_block_number, endBlockNumber=end_block_number)
        response = self._deploy_stub.getBlocksByHeights(query, timeout=self.timeout)
        result = self._handle_stream(response)
        return list(map(lambda x: x.blockInfo, result))  # type: ignore

    def get_continuation(self, par: Par, depth: int = 1) -> ContinuationAtNameResponse:
        query = ContinuationAtNameQuery(depth=depth, names=[par])
        response = self._deploy_stub.listenForContinuationAtName(query, timeout=self.timeout)
        self._check_response(response)
        return response

    def previewPrivateNames(self, public_key: PublicKey, timestamp: int, nameQty: int) -> PrivateNamePreviewResponse:
        query = PrivateNamePreviewQuery(user=public_key.to_bytes(), timestamp=timestamp, nameQty=nameQty)
        response = self._deploy_stub.previewPrivateNames(query, timeout=self.timeout)
        self._check_response(response)
        return response

    def get_event_data(self, block_hash: str, force_replay: bool = False) -> EventInfoResponse:
        """Get block execution trace (COMM/produce/consume events per deploy)."""
        query = ReportQuery(hash=block_hash, forceReplay=force_replay)
        response = self._deploy_stub.getEventByHash(query, timeout=self.timeout)
        self._check_response(response)
        return response

    def visual_dag(self, depth: int, showJustificationLines: bool, startBlockNumber: int) -> str:
        query = VisualizeDagQuery(depth=depth, showJustificationLines=showJustificationLines,
                                  startBlockNumber=startBlockNumber)
        response = self._deploy_stub.visualizeDag(query, timeout=self.timeout)
        result = self._handle_stream(response)
        return ''.join(list(map(lambda x: x.content, result)))  # type: ignore

    def get_transaction(self, block_hash: str) -> List[DeployWithTransaction]:
        if self.param is None:
            raise ValueError("You haven't install your network param.")
        transactions = []
        event_data = self.get_event_data(block_hash)
        deploys = event_data.result.deploys
        for deploy in deploys:
            # it is possible that the user deploy doesn't generate
            # any comm events . So there are only two report in the response.
            if len(deploy.report) == 2:
                continue
            # normally there are precharge, user and refund deploy, 3 totally.
            elif len(deploy.report) == 3:
                precharge = deploy.report[0]
                user = deploy.report[1]
                refund = deploy.report[2]
                report = Report(precharge, user, refund)
                transactions.append(
                    DeployWithTransaction(
                        deploy.deployInfo,
                        find_transfer_comm(report.user, self.param.transfer_unforgeable))
                )
        return transactions


def find_transfer_comm(report: SingleReport, transfer_template_unforgeable: Par) -> List[Transaction]:
    transfers = []
    transactions = []
    for event in report.events:
        report_type = event.WhichOneof('report')
        if report_type == 'comm':
            channel = event.comm.consume.channels[0]
            if channel == transfer_template_unforgeable:
                transfers.append(event)
                from_addr = event.comm.produces[0].data.pars[0].exprs[0].g_string
                to_addr = event.comm.produces[0].data.pars[2].exprs[0].g_string
                amount = event.comm.produces[0].data.pars[3].exprs[0].g_int
                ret = event.comm.produces[0].data.pars[5]
                transactions.append(Transaction(from_addr, to_addr, amount, ret, None))
    for transaction in transactions:
        for event in report.events:
            report_type = event.WhichOneof('report')
            if report_type == 'produce':
                channel = event.produce.channel
                if channel == transaction.ret_unforgeable:
                    data = event.produce.data
                    # transfer result True or False
                    result = data.pars[0].exprs[0].e_tuple_body.ps[0].exprs[0].g_bool
                    if result:
                        reason = ''
                    else:
                        reason = data.pars[0].exprs[0].e_tuple_body.ps[1].exprs[0].g_string
                    transaction.success = (result, reason)
        if transaction.success is None:
            transaction.success = (True,
                                   'Possibly the transfer toAddr wallet is not created in chain. Create the wallet to make transaction succeed.')
    return transactions
