use anyhow::{Context, Result};
use quanta::Clock;
use std::{
    env,
    process::{Command, Output},
};
use tracing::*;
use tracing_subscriber::*;
use serde_json::Value;

struct WasmerEnv {
    wasmer_command: Command,
    wasmer_js: String,
}

impl WasmerEnv {
    fn new() -> Result<WasmerEnv> {
        let wasmer_bin = env::var("WASMER_BIN").context("Cannot read env var WASMER_BIN")?;
        let wasmer_command = Command::new(wasmer_bin);
        let wasmer_js = env::var("WASMER_JS").context("Cannot read env var WASMER_JS")?;
        Ok(WasmerEnv {
            wasmer_command,
            wasmer_js,
        })
    }

    fn invoke_js(&mut self, script: &str) -> Result<Output> {
        self.wasmer_command
            .arg(&self.wasmer_js)
            .arg("--")
            .arg("--std")
            .arg("-e")
            .arg(script);

        self.wasmer_command
            .output()
            .context("Cannot execute quickJS")
    }

    fn invoke_and_parse(&mut self, script: &str, user_input: Value, resource_pool_properties: Value,
                        resource_pool: Value, current_resources: Vec<Value>, function_call: &str)
                        -> Result<Value> {
        let mut header = "
        console.error = function(...args) {
            std.err.puts(args.join(' '));
            std.err.puts('\\n');
        }
        const log = console.error;
        ".to_owned();

        header += &Self::add_js_var("userInput", user_input)?;
        header += &Self::add_js_var("resourcePoolProperties", resource_pool_properties)?;
        header += &Self::add_js_var("resourcePool", resource_pool)?;
        header += &Self::add_js_var("currentResources", Value::Array(current_resources))?;

        let footer = format!("\nlet result = {};\n", function_call) + "
        if (result != null) {
            if (typeof result === 'object') {
                result = JSON.stringify(result);
            }
            std.out.puts(result);
        }
        ";
        let script = header + script + &footer;
        debug!("Executing script:\n{}", script);
        let output = self.invoke_js(&script)?;
        trace!("Output {:?}", output);
        let val: Value = serde_json::from_slice(&output.stdout)
            .context(format!("Cannot deserialize '{:?}'", output.stdout))?;
        Ok(val)
    }

    fn add_js_var(name: &str, val: Value) -> Result<String> {
        let serialized = serde_json::to_string(&val)?;
        Ok(format!("const {} = {};\n", name, &serialized))
    }
}

fn main() -> Result<()> {
    let clock = Clock::new();
    let start = clock.start();
    let fmt_event = tracing_subscriber::fmt::format::Format::default().with_target(false);
    tracing_subscriber::fmt()
        .event_format(fmt_event)
        .with_env_filter(EnvFilter::from_default_env())
        .init();
    let end = clock.end();
    trace!("Initialized in {:?}", clock.delta(start, end));
    let start = clock.start();

    let span = span!(Level::INFO, "my_span");
    let _enter = span.enter();


    let end = clock.end();
    trace!("Finished in {:?}", clock.delta(start, end));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::Once;

    static START: Once = Once::new();

    fn initialize_logging() {
        START.call_once(|| {
            let fmt_event = tracing_subscriber::fmt::format::Format::default().with_target(false);
            tracing_subscriber::fmt()
                .event_format(fmt_event)
                .with_env_filter(EnvFilter::from_default_env())
                .init();
        });
    }

    #[test]
    fn invoke_js() -> Result<()> {
        initialize_logging();
        debug!("test");
        let mut wasmer_env = WasmerEnv::new()?;
        let output = wasmer_env.invoke_js("console.log(2+2)")?;
        trace!("{:?}", output);
        assert_eq!("4\n", String::from_utf8(output.stdout)?);
        Ok(())
    }

    #[test]
    fn invoke_and_parse() -> Result<()> {
        initialize_logging();
        let mut wasmer_env = WasmerEnv::new()?;
        let script = "function invoke() {\
        return {mykey:1, userInput, resourcePoolProperties, resourcePool, currentResources}\
        }";
        let user_input = json!({
            "input": "input"
        });
        let resource_pool_properties = json!({
            "rpp":"rpp"
        });
        let resource_pool = json!({
            "rp":"rp"
        });
        let current_resources = json!([
            "res1", "res2"
        ]).as_array().ok_or(anyhow::anyhow!("Unexpected"))?.to_owned();

        let actual = wasmer_env.invoke_and_parse(script, user_input, resource_pool_properties,
                                                 resource_pool, current_resources, "invoke()")?;
        let expected = json!({
            "mykey": 1,
            "userInput": {"input":"input"},
            "resourcePoolProperties": {"rpp":"rpp"},
            "resourcePool": {"rp":"rp"},
            "currentResources": ["res1", "res2"]
        });
        assert_eq!(expected, actual);
        Ok(())
    }
}
