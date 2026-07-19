pub mod jobs;
pub mod twap;
pub mod vwap;

pub async fn handle_worker(job_id: &str) -> anyhow::Result<()> {
    let job = crate::runtime::get_strategy_job_from_running_daemon(job_id).await?;
    match job.definition {
        crate::strategies::jobs::StrategyJobDefinition::Twap(_) => {
            twap::handle_worker_job(job_id, job).await
        }
        crate::strategies::jobs::StrategyJobDefinition::Vwap(_) => {
            vwap::handle_worker_job(job_id, job).await
        }
    }
}
