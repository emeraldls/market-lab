pub mod jobs;
pub mod mid_price;

pub async fn handle_worker(job_id: &str) -> anyhow::Result<()> {
    let job = crate::runtime::get_bot_job_from_running_daemon(job_id).await?;
    match job.definition {
        crate::bots::jobs::BotJobDefinition::MidPrice(_)
        | crate::bots::jobs::BotJobDefinition::VolumeMid(_) => {
            mid_price::handle_worker_job(job_id, job).await
        }
    }
}
