use Install::setting::JUDGEMENT;
use Install::setting::local_config::SUPER_URL;
use Static::Events;

///# 测试db
pub async fn test_get_db<'life>() -> Events<Vec<&'life str>> {
    let mut mn = vec![];
    for i in JUDGEMENT.into_iter() {
        let x = SUPER_URL.load().postgres.connect_rab_execute(i.0).await?;
        if x.rows_affected == 0 {
            mn.push(i.1);
        }
    }
    Ok(mn)
}