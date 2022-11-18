use crate::*;

impl CommandService for Hget {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        match store.get(&self.table, &self.key) {
            Ok(Some(value)) => value.into(),
            Ok(None) => KvError::NotFound(self.table, self.key).into(),
            Err(e) => e.into(),
        }
    }
}

impl CommandService for Hset {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        match self.pair {
            Some(pair) => match store.set(&self.table, pair.key, pair.value.unwrap_or_default()) {
                Ok(Some(value)) => value.into(),
                Ok(None) => Value::default().into(),
                Err(e) => e.into(),
            },
            None => Value::default().into(),
        }
    }
}

impl CommandService for Hgetall {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        match store.get_all(&self.table) {
            Ok(pairs) => pairs.into(),
            Err(e) => e.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::command_request::RequestData;
    use crate::service;
    use super::*;

    #[test]
    fn hset_should_work() {
        let store = MemTable::new();
        let request = CommandRequest::new_hset("t1", "hello", "world".into());
        let response = dispatch(request.clone(), &store);
        service::assert_response_ok(response, &[Value::default()], &[]);

        let response = dispatch(request, &store);
        service::assert_response_ok(response, &["world".into()], &[]);
    }

    #[test]
    fn hget_should_work() {
        let store = MemTable::new();
        let request = CommandRequest::new_hset("score", "math", 10.into());
        let response = dispatch(request, &store);
        service::assert_response_ok(response, &[Value::default()], &[]);

        let request = CommandRequest::new_hget("score", "math");
        let response = dispatch(request, &store);
        service::assert_response_ok(response, &[10.into()], &[]);
    }

    #[test]
    fn hget_with_non_existing_key_should_return_404() {
        let store = MemTable::new();
        let request = CommandRequest::new_hget("score", "math");
        let response = dispatch(request, &store);
        service::assert_response_error(response, 404, "Not found");
    }

    #[test]
    fn hget_all_should_work() {
        let store = MemTable::new();
        let cmds = vec![
            CommandRequest::new_hset("score", "math", 10.into()),
            CommandRequest::new_hset("score", "english", 20.into()),
            CommandRequest::new_hset("score", "chinese", 30.into()),
            CommandRequest::new_hset("score", "math", 40.into()),
        ];

        for cmd in cmds {
            dispatch(cmd, &store);
        }

        let request = CommandRequest::new_hget_all("score");
        let response = dispatch(request, &store);

        let pairs = vec![
            KvPair::new("chinese", 30.into()),
            KvPair::new("english", 20.into()),
            KvPair::new("math", 40.into()),
        ];
        service::assert_response_ok(response, &[], &pairs);
    }
}