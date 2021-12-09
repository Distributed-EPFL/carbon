use doomstack::Doom;

#[derive(Doom)]
pub(in crate::processing::processor::commit) enum ServeCommitError {
    #[doom(description("Connection error"))]
    ConnectionError,
    #[doom(description("Unexpected request"))]
    UnexpectedRequest,
    #[doom(description("Invalid batch"))]
    InvalidBatch,
}
