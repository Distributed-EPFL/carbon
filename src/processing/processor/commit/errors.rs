use doomstack::Doom;

#[derive(Doom)]
pub(in crate::processing::processor::commit) enum ServeCommitError {
    #[doom(description("Connection error"))]
    ConnectionError,
    #[doom(description("Unexpected request"))]
    UnexpectedRequest,
    #[doom(description("Malformed batch"))]
    MalformedBatch,
    #[doom(description("Database void"))]
    DatabaseVoid,
    #[doom(description("Invalid batch"))]
    InvalidBatch,
    #[doom(description("Malformed commit proofs"))]
    MalformedCommitProofs,
    #[doom(description("Invalid commit proof"))]
    InvalidCommitProof,
    #[doom(description("Malformed dependencies"))]
    MalformedDependencies,
    #[doom(description("Mismatched dependency"))]
    MismatchedDependency,
    #[doom(description("Invalid dependency"))]
    InvalidDependency,
    #[doom(description("Batch inapplicable"))]
    BatchInapplicable,
    #[doom(description("`BatchCompletion` invalid"))]
    BatchCompletionInvalid,
}
