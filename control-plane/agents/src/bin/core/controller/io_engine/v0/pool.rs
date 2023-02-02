use common_lib::{
    transport_api::ResourceKind,
    types::v0::transport::{CreatePool, DestroyPool, NodeId, PoolState},
};
use rpc::io_engine::Null;
use snafu::ResultExt;

use agents::{
    errors::{GrpcRequest as GrpcRequestError, SvcError},
    msg_translation::v0::{
        rpc_pool_to_agent as v0_rpc_pool_to_agent, AgentToIoEngine as v0_conversion,
    },
};

#[async_trait::async_trait]
impl crate::controller::io_engine::PoolListApi for super::RpcClient {
    async fn list_pools(&self, id: &NodeId) -> Result<Vec<PoolState>, SvcError> {
        let rpc_pools = self
            .client()
            .list_pools(Null {})
            .await
            .context(GrpcRequestError {
                resource: ResourceKind::Pool,
                request: "list_pools",
            })?;

        let rpc_pools = &rpc_pools.get_ref().pools;

        let pools = rpc_pools
            .iter()
            .map(|p| v0_rpc_pool_to_agent(p, id))
            .collect();

        Ok(pools)
    }
}

#[async_trait::async_trait]
impl crate::controller::io_engine::PoolApi for super::RpcClient {
    async fn create_pool(&self, request: &CreatePool) -> Result<PoolState, SvcError> {
        let rpc_pool = self
            .client()
            .create_pool(v0_conversion::to_rpc(request))
            .await
            .context(GrpcRequestError {
                resource: ResourceKind::Pool,
                request: "create_pool",
            })?;
        let pool = v0_rpc_pool_to_agent(&rpc_pool.into_inner(), &request.node);
        Ok(pool)
    }

    async fn destroy_pool(&self, request: &DestroyPool) -> Result<(), SvcError> {
        let _ = self
            .client()
            .destroy_pool(v0_conversion::to_rpc(request))
            .await
            .context(GrpcRequestError {
                resource: ResourceKind::Pool,
                request: "destroy_pool",
            })?;
        Ok(())
    }
}
