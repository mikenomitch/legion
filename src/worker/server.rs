use crate::prelude::*;

use crate::structs::{worker_server::Worker, PostWorkReply, PostWorkRequest};
use tonic::{Request, Response, Status};

#[derive(Debug)]
pub struct WorkerAgentService {}
#[tonic::async_trait]
impl Worker for WorkerAgentService {
    #[instrument(err, skip_all)]
    async fn post_work(
        &self,
        request: Request<PostWorkRequest>,
    ) -> Result<Response<PostWorkReply>, Status> {
        info!("Got a request from {:?}", request.remote_addr());

        let reply = PostWorkReply {
            message: "I got work yay!".into(),
        };

        Ok(Response::new(reply))
    }
}
