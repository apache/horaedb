use thiserror::Error;

#[derive(Debug, Error)]
#[error(transparent)]
pub struct Error(#[from] InnerError);

impl From<anyhow::Error> for Error {
    fn from(source: anyhow::Error) -> Self {
        Self(InnerError::Other { source })
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum ErrorKind {
    KeyTooLarge,
    Internal,
}

impl Error {
    pub fn kind(&self) -> ErrorKind {
        match self.0 {
            InnerError::KeyTooLarge { .. } => ErrorKind::KeyTooLarge,
            InnerError::Other { .. } => ErrorKind::Internal,
        }
    }
}

#[macro_export]
macro_rules! ensure {
    ($cond:expr, $msg:expr) => {
        if !$cond {
            return Err(anyhow::anyhow!($msg).into());
        }
    };
}

#[derive(Error, Debug)]
pub(crate) enum InnerError {
    #[error("too large key, max:{max}, current:{current}")]
    KeyTooLarge { current: usize, max: usize },

    #[error(transparent)]
    Other {
        #[from]
        source: anyhow::Error,
    },
}
