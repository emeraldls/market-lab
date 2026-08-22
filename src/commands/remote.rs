use anyhow::{Result, bail};
use serde::Serialize;

use crate::cli::{OutputFormat, RemoteAddArgs, RemoteNameArgs, RemoteOutputArgs, RemoteUseArgs};
use crate::remote::{self, RemoteConfig, RemoteHandshake, RemoteProfile};

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct RemoteStatus<'a> {
    active: &'a str,
    profile: Option<&'a RemoteProfile>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct RemoteTest<'a> {
    name: &'a str,
    ssh: &'a str,
    connected: bool,
    #[serde(flatten)]
    handshake: &'a RemoteHandshake,
}

pub async fn handle_add(args: RemoteAddArgs) -> Result<()> {
    validate_output(args.output)?;
    remote::validate_name(&args.name)?;
    let profile = RemoteProfile {
        ssh: args.ssh,
        mlab: args.mlab,
    };
    remote::validate_profile(&profile)?;
    let mut config = remote::load()?;
    config.remotes.insert(args.name.clone(), profile.clone());
    if args.use_now {
        config.active = Some(args.name.clone());
    }
    remote::save(&config)?;
    match args.output {
        OutputFormat::Terminal => {
            println!("remote `{}` saved", args.name);
            println!("  ssh:    {}", profile.ssh);
            println!("  mlab:   {}", profile.mlab);
            println!("  active: {}", if args.use_now { "yes" } else { "no" });
            if !args.use_now {
                println!("  select: mlab remote use {}", args.name);
            }
        }
        OutputFormat::Json => println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "name": args.name,
                "profile": profile,
                "active": args.use_now,
            }))?
        ),
        OutputFormat::Jsonl => println!(
            "{}",
            serde_json::to_string(&serde_json::json!({
                "name": args.name,
                "profile": profile,
                "active": args.use_now,
            }))?
        ),
        OutputFormat::Csv | OutputFormat::Parquet => unreachable!(),
    }
    Ok(())
}

pub fn handle_list(args: RemoteOutputArgs) -> Result<()> {
    validate_output(args.output)?;
    let config = remote::load()?;
    match args.output {
        OutputFormat::Terminal => {
            if config.remotes.is_empty() {
                println!("no SSH remotes configured");
                println!("select one: mlab remote use <user@host>");
            } else {
                println!("NAME\tSSH\tMLAB\tACTIVE");
                for (name, profile) in &config.remotes {
                    println!(
                        "{}\t{}\t{}\t{}",
                        name,
                        profile.ssh,
                        profile.mlab,
                        if config.active.as_deref() == Some(name) {
                            "yes"
                        } else {
                            ""
                        }
                    );
                }
            }
        }
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&config)?),
        OutputFormat::Jsonl => println!("{}", serde_json::to_string(&config)?),
        OutputFormat::Csv | OutputFormat::Parquet => unreachable!(),
    }
    Ok(())
}

pub fn handle_use(args: RemoteUseArgs) -> Result<()> {
    validate_output(args.output)?;
    let mut config = remote::load()?;
    let profile = if args.name == "local" {
        config.active = None;
        None
    } else {
        let profile = remote::remember_target(&mut config, &args.name)?;
        config.active = Some(args.name.clone());
        Some(profile)
    };
    remote::save(&config)?;
    render_status(&config, profile.as_ref(), args.output)
}

pub fn handle_status(args: RemoteOutputArgs) -> Result<()> {
    validate_output(args.output)?;
    let config = remote::load()?;
    let profile = config
        .active
        .as_ref()
        .and_then(|name| config.remotes.get(name));
    render_status(&config, profile, args.output)
}

pub async fn handle_test(args: RemoteNameArgs) -> Result<()> {
    validate_output(args.output)?;
    if args.name == "local" {
        bail!("`local` does not use SSH; run `mlab daemon status` instead");
    }
    let config = remote::load()?;
    let profile = remote::resolve_target(&config, &args.name)?;
    let handshake = remote::test(&args.name, &profile).await?;
    let result = RemoteTest {
        name: &args.name,
        ssh: &profile.ssh,
        connected: true,
        handshake: &handshake,
    };
    match args.output {
        OutputFormat::Terminal => {
            println!("remote `{}` connected", args.name);
            println!("  ssh:              {}", profile.ssh);
            println!("  mlab:             {}", handshake.marketlab_version);
            println!("  transport:        {}", handshake.transport_version);
            println!("  runtime protocol: {}", handshake.runtime_version);
            println!("  daemon backend:   {}", handshake.daemon_backend.as_str());
            println!("  platform:         {}/{}", handshake.os, handshake.arch);
        }
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&result)?),
        OutputFormat::Jsonl => println!("{}", serde_json::to_string(&result)?),
        OutputFormat::Csv | OutputFormat::Parquet => unreachable!(),
    }
    Ok(())
}

pub fn handle_remove(args: RemoteNameArgs) -> Result<()> {
    validate_output(args.output)?;
    let mut config = remote::load()?;
    if config.remotes.remove(&args.name).is_none() {
        bail!("remote `{}` is not configured", args.name);
    }
    let was_active = config.active.as_deref() == Some(args.name.as_str());
    if was_active {
        config.active = None;
    }
    remote::save(&config)?;
    match args.output {
        OutputFormat::Terminal => {
            println!("remote `{}` removed", args.name);
            if was_active {
                println!("active target: local");
            }
        }
        OutputFormat::Json => println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "removed": args.name,
                "active": "local",
            }))?
        ),
        OutputFormat::Jsonl => println!(
            "{}",
            serde_json::to_string(&serde_json::json!({
                "removed": args.name,
                "active": "local",
            }))?
        ),
        OutputFormat::Csv | OutputFormat::Parquet => unreachable!(),
    }
    Ok(())
}

fn render_status(
    config: &RemoteConfig,
    profile: Option<&RemoteProfile>,
    output: OutputFormat,
) -> Result<()> {
    let active = config.active.as_deref().unwrap_or("local");
    let status = RemoteStatus { active, profile };
    match output {
        OutputFormat::Terminal => {
            println!("MarketLab target: {active}");
            if let Some(profile) = profile {
                println!("  transport: ssh");
                println!("  ssh:       {}", profile.ssh);
                println!("  mlab:      {}", profile.mlab);
            } else {
                println!("  transport: local");
            }
        }
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&status)?),
        OutputFormat::Jsonl => println!("{}", serde_json::to_string(&status)?),
        OutputFormat::Csv | OutputFormat::Parquet => unreachable!(),
    }
    Ok(())
}

fn validate_output(output: OutputFormat) -> Result<()> {
    if matches!(output, OutputFormat::Csv | OutputFormat::Parquet) {
        bail!("remote commands support only --output terminal|json|jsonl");
    }
    Ok(())
}
