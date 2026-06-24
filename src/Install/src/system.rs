use std::fmt::{Display, Formatter};
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;

use anyhow::Result;
use quick_xml::{de, se};
use serde::{Deserialize, Serialize};
use serde_json_borrow::Value;
use sysinfo::{CpuExt, System, SystemExt};

///# 设置工具
pub trait InstallUtils: Serialize + for<'life> Deserialize<'life> {
	///# 缓存构建
	fn buff_read_string(e: &Path) -> Result<String> {
		let mut x = BufReader::new(File::open(e)?);
		let mut et = String::new();
		x.read_to_string(&mut et)?;
		Ok(et)
	}
	///# 缓存构建
	fn buff_write_string(e: &Path, tet: String) -> Result<()> {
		BufWriter::new(File::open(e)?).write_all(tet.as_bytes())?;
		Ok(())
	}
}

pub trait Yaml: InstallUtils {
	///# yaml
	//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
	fn yaml_build_unknown(e: &Path) -> Result<serde_yaml::Value> {
		Ok(serde_yaml::from_str(Self::buff_read_string(Path::new(e))?.as_str())?)
	}
	fn yaml_build(e: &Path) -> Result<Self> {
		Ok(serde_yaml::from_str(
			Self::buff_read_string(Path::new(e))?.as_str(),
		)?)
	}
	fn yaml_update(self, e: &Path) -> Result<Self> {
		Self::buff_write_string(e, serde_yaml::to_string(&self)?)?;
		Ok(self)
	}
}

pub trait Toml: InstallUtils {
	///# toml
	//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
	fn toml_build_unknown(e: &Path) -> Result<toml::Value> {
		Ok(toml::from_str(Self::buff_read_string(e)?.as_str())?)
	}
	fn toml_build(e: &Path) -> Result<Self> {
		Ok(toml::from_str(
			Self::buff_read_string(Path::new(e))?.as_str(),
		)?)
	}
	fn toml_update(self, e: &Path) -> Result<Self> {
		Self::buff_write_string(e, toml::to_string(&self)?)?;
		Ok(self)
	}
}

pub trait Xml: InstallUtils {
	///# xml
	//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
	///# 构建
	fn xml_build(e: &Path) -> Result<Self> {
		Ok(de::from_str(&Self::buff_read_string(e)?)?)
	}
	///# 更新
	fn xml_update(self, e: &Path) -> Result<Self> {
		Self::buff_write_string(e, se::to_string(&self)?)?;
		Ok(self)
	}
}

pub trait Json: InstallUtils {
	///# json
	fn build_unknown(e: &str) -> Result<Value> {
		Ok(serde_json::from_str(e)?)
	}
	///# 转换
	fn json_build(e: &Path) -> Result<Self> {
		Ok(serde_json::from_str(
			Self::buff_read_string(Path::new(e))?.as_str(),
		)?)
	}
	///# 更新
	fn json_update(self, e: &Path) -> Result<Self> {
		Self::buff_write_string(e, serde_json::to_string(&self)?)?;
		Ok(self)
	}
}

///# 系统状态 cpu ram
#[derive(Debug, Serialize, Deserialize)]
pub struct InfSystem {
	pub cpu: f32,
	pub ram: u64,
}

impl InfSystem {
	///# 返回剩余GB
	pub const fn ram_gb(self) -> u64 {
		self.ram / 1024 / 1024 / 1024
	}
	///# 返回剩余MB
	pub const fn ram_mb(self) -> u64 {
		self.ram / 1024 / 1024
	}
	///# 返回剩余KB
	pub const fn ram_kb(self) -> u64 {
		self.ram / 1024
	}
}

impl Display for InfSystem {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		writeln!(
			f,
			"占用cpu:{}% rpm剩余:{}MB",
			self.cpu,
			self.ram / 1024 / 1024
		)
	}
}

impl Default for InfSystem {
	fn default() -> Self {
		let mut sys = System::new_all();
		let ram = sys.total_memory() - sys.used_memory();
		sys.cpus().iter().for_each(|x| {
			x.cpu_usage();
		});
		let mut b20 = 0.0;
		sys.refresh_all();
		sys.cpus().iter().for_each(|x| {
			b20 += x.cpu_usage();
		});
		let cpu = b20 / sys.cpus().len() as f32;
		InfSystem { cpu, ram }
	}
}