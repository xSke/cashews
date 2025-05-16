use serde::Deserialize;

#[derive(Deserialize, Debug)]
pub struct MmolbState {
    #[serde(rename = "GreaterLeagues")]
    pub greater_leagues: Vec<String>,

    #[serde(rename = "LesserLeagues")]
    pub lesser_leagues: Vec<String>,
}

#[derive(Deserialize, Debug)]
pub struct MmolbLeague {
    #[serde(rename = "Teams")]
    pub teams: Vec<String>,

    #[serde(rename = "SuperstarTeam")]
    pub superstar_team: Option<String>,
}

#[derive(Deserialize, Debug)]
pub struct MmolbTeam {
    #[serde(rename = "Location")]
    pub location: String,

    #[serde(rename = "Name")]
    pub name: String,

    #[serde(rename = "Players")]
    pub players: Vec<MmolbTeamPlayer>,
}

#[derive(Deserialize, Debug)]
pub struct MmolbTeamPlayer {
    #[serde(rename = "PlayerID")]
    pub player_id: String,
}

#[derive(Deserialize, Debug)]
pub struct MmolbPlayer {
    #[serde(rename = "FirstName")]
    pub first_name: String,

    #[serde(rename = "LastName")]
    pub last_name: String,
}
