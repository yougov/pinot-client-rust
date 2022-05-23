use rand::prelude::*;

pub fn random_element<T>(list: &Vec<T>) -> Option<&T> {
    if !list.is_empty() {
        Some(&list[rand::thread_rng().gen_range(0..list.len())])
    } else {
        None
    }
}

pub fn clone_random_element<T: Clone>(list: &Vec<T>) -> Option<T> {
    random_element(list).cloned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn random_element_returns_an_element_from_list() {
        let list: Vec<u64> = vec![1, 2, 3];
        let option = random_element(&list);
        assert!(option.is_some());
        assert!(list.contains(option.unwrap()));
    }

    #[test]
    fn random_element_returns_none_for_empty_list() {
        let list: Vec<u64> = vec![];
        let option = random_element(&list);
        assert!(option.is_none());
    }

    #[test]
    fn clone_random_element_returns_a_cloned_element_from_list() {
        let list: Vec<u64> = vec![1, 2, 3];
        let option = clone_random_element(&list);
        assert!(option.is_some());
        assert!(list.contains(&option.unwrap()));
    }

    #[test]
    fn clone_random_element_returns_none_for_empty_list() {
        let list: Vec<u64> = vec![];
        let option = clone_random_element(&list);
        assert!(option.is_none());
    }
}