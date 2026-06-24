use ssh::{create_session, SessionBroker};
use std::net::SocketAddr;
use std::path::Path;
use crate::static_array::Archive;

///# shh
pub struct SSH {
    //ip
    pub hostname: SocketAddr,
    //登录需求
    pub state: SSHState,
}
///# 登录需求
pub enum SSHState {
    FileKeys(&'static Path),
    LogIn { name: String, password: String },
}

impl SSHClient for SSH {
    fn init() -> SSH {
        SSH {
            hostname: "192.168.100.10:22".parse().unwrap(),
            state: SSHState::LogIn {
                name: "root".to_string(),
                password: "000000".to_string(),
            },
        }
    }
}
///# ssh 链接
pub trait SSHClient {
    fn init() -> SSH;
    fn ssh() -> SessionBroker {
        let SSH { hostname, state } = <Self as SSHClient>::init();
        match state {
            SSHState::FileKeys(e) => create_session()
                .private_key_path(e)
                .connect(hostname)
                .unwrap()
                .run_backend(),
            SSHState::LogIn { name, password } => create_session()
                .username(name.as_str())
                .password(password.as_str())
                .connect(hostname)
                .unwrap()
                .run_backend(),
        }
    }
    ///# 上传文件 本地 -> 远程
    fn run_scp_pull(local: &str, end: &str) {
        let mut er = Self::ssh();
        let scp = er.open_scp().unwrap();
        scp.upload(local, end).unwrap();
        er.close();
    }
    ///# 下载文件 远程 -> 本地
    fn run_scp_get(end: &str, local: &str) {
        let mut er = Self::ssh();
        let mut scp = er.open_scp().unwrap();
        scp.start_download(local, end).unwrap();
        scp.end_download().unwrap();
        er.close();
    }
    ///# 发送指令
    fn run_ssh<const ER: usize>(shell: Archive<&str, ER>) {
        let mut er = Self::ssh();
        let mut et = er.open_exec().unwrap();
        shell.into_iter().for_each(|x| { et.send_command(x).unwrap(); });
        er.close();
    }
}
