
use std::fmt::{Display, Formatter};
use std::io;

use comfy_table::{Attribute, Cell, Color, ContentArrangement, Table};
use comfy_table::modifiers::UTF8_ROUND_CORNERS;
use comfy_table::presets::UTF8_FULL;
use dialoguer::{Confirm, FuzzySelect, Input, MultiSelect, Password, Select};
use indicatif::ProgressBar;

/*
单独模块不接受其他库
*/

///#风格
pub enum Colour {
    ///错误
    Error,
    ///输出
    Output,
    ///命令
    Order,
    ///监控
    Monitoring,
    ///函数
    Function,
}

impl Display for Colour {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Colour::Error => { write!(f, "ERROR") }
            Colour::Output => { write!(f, "OUTPUT") }
            Colour::Order => { write!(f, "ORDER") }
            Colour::Monitoring => { write!(f, "MONITORING") }
            Colour::Function => { write!(f, "FUNCTION") }
        }
    }
}

///#数据
pub struct Information<EF: Display, ER: IntoIterator<Item=EF>, GL: IntoIterator<Item=ER>> {
    ///列表
    pub list: ER,
    ///数据
    pub data: GL,
}

///# 显示
pub trait ViewDrive {
    type Frame;
    fn view(&self) -> Self::Frame;
    fn table<EF: Display, ER: IntoIterator<Item=EF>, GL: IntoIterator<Item=ER>>(
        &self,
        _: Information<EF, ER, GL>,
    ) -> Table where Self: Display {
        println!("{}", self);
        Table::new()
    }
    ///# 选择
    fn view_container(e: &str) -> io::Result<bool> {
        Confirm::new().with_prompt(e).interact().map_err(into_io_error)
    }
    ///# 进度条
    fn view_column(e: u64) -> ProgressBar {
        ProgressBar::new(e)
    }
    ///# 选择输入
    fn select_column(position: &[&str], rows: &str) -> io::Result<usize> {
        Select::new().items(position).with_prompt(rows).interact().map_err(into_io_error)
    }
    ///# 密码
    fn password_column(position: &str) -> io::Result<String> {
        Password::new().with_prompt(position).interact().map_err(into_io_error)
    }
    ///# 多选择
    fn select_multi_column(position: &[&str], rows: &str) -> io::Result<Vec<usize>> {
        MultiSelect::new().items(position).with_prompt(rows).interact().map_err(into_io_error)
    }
    ///# 选择
    fn select_func_column(position: &[&str], rows: &str) -> io::Result<usize> {
        FuzzySelect::new().items(position).with_prompt(rows).interact().map_err(into_io_error)
    }
    ///# 输入
    fn input_column(position: &str) -> io::Result<String> {
        Input::new().with_prompt(position).interact().map_err(into_io_error)
    }
    fn input_column_def(position: &str, def: &str) -> io::Result<String> {
        Input::new().with_prompt(position).with_initial_text(def).interact().map_err(into_io_error)
    }
}

fn into_io_error(e: dialoguer::Error) -> io::Error {
    io::Error::new(io::ErrorKind::Other, e)
}

impl ViewDrive for Colour {
    type Frame = Frames;
    fn view(&self) -> Self::Frame {
        match self {
            Colour::Error => Frames {
                text: Attribute::Italic,
                frames: Color::DarkRed,
            },
            Colour::Output => Frames {
                text: Attribute::Bold,
                frames: Color::DarkGreen,
            },
            Colour::Order => Frames {
                text: Attribute::RapidBlink,
                frames: Color::DarkYellow,
            },
            Colour::Monitoring => Frames {
                text: Attribute::Underlined,
                frames: Color::DarkCyan,
            },
            Colour::Function => Frames {
                text: Attribute::Reverse,
                frames: Color::DarkGrey,
            },
        }
    }
    fn table<EF: Display, ER: IntoIterator<Item=EF>, GL: IntoIterator<Item=ER>>(
        &self,
        e: Information<EF, ER, GL>,
    ) -> Table {
        let i = Colour::view(self);
        let mut table = Table::new();
        table
            .load_preset(UTF8_FULL)
            .apply_modifier(UTF8_ROUND_CORNERS)
            .set_content_arrangement(ContentArrangement::Dynamic);
        table.set_header(
            e.list
                .into_iter()
                .map(|x| Cell::new(x).add_attribute(i.text).fg(i.frames))
                .collect::<Vec<_>>(),
        );
        e.data.into_iter().for_each(|x| {
            table.add_row(
                x.into_iter()
                    .map(|x| Cell::new(x).add_attribute(i.text).fg(i.frames))
                    .collect::<Vec<_>>(),
            );
        });
        table
    }
}

///#画面数据
pub struct Frames {
    //文本
    text: Attribute,
    //单元格前景色
    frames: Color,
}
