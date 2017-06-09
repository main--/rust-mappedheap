use std::ops::Deref;

pub enum MaybeMut<'a, T: 'a> {
    Ref(&'a T),
    MutRef(&'a mut T),
}

impl<'a, T: 'a> From<&'a mut T> for MaybeMut<'a, T> {
    fn from(r: &'a mut T) -> Self {
        MaybeMut::MutRef(r)
    }
}

impl<'a, T: 'a> From<&'a T> for MaybeMut<'a, T> {
    fn from(r: &'a T) -> Self {
        MaybeMut::Ref(r)
    }
}

impl<'a, 'b, T: 'a> MaybeMut<'a, T> {
    pub fn borrow_mut(&'b mut self) -> Option<&'b mut T> {
        match self {
            &mut MaybeMut::Ref(_) => None,
            &mut MaybeMut::MutRef(ref mut r) => Some(r),
        }
    }
}

impl<'a, T: 'a> Deref for MaybeMut<'a, T> {
    type Target = T;

    fn deref(&self) -> &T {
        match self {
            &MaybeMut::Ref(r) => r,
            &MaybeMut::MutRef(ref r) => r,
        }
    }
}
