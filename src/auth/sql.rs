#[derive(Debug, Clone)]
pub struct SqlAuth {
    pub username: String,
    pub password: String,
}

impl SqlAuth {
    pub fn new(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self {
            username: username.into(),
            password: password.into(),
        }
    }
}
