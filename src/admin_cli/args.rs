#[derive(Debug, Clone)]
pub struct AddUserArgs<'a> {
    pub access_key: &'a str,
    pub secret_key: &'a str,
}
