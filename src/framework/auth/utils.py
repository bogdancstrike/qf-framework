
def split_by_crlf(s):
    return [v for v in s.splitlines() if v]


def password_check(passwd):
    SpecialSym = ['$', '@', '#', '%', '!', ',', '.', '?']
    val = True

    if len(passwd) < 6:
        return {'success': False, 'msg': 'length should be at least 6'}

    if len(passwd) > 20:
        return {'success': False, 'msg': 'length should be less than 20'}

    if not any(char.isdigit() for char in passwd):
        return {'success': False, 'msg': 'Password should have at least one numeral'}

    if not any(char.isupper() for char in passwd):
        return {'success': False, 'msg': 'Password should have at least one uppercase letter'}

    if not any(char.islower() for char in passwd):
        return {'success': False, 'msg': 'Password should have at least one lowercase letter'}

    if not any(char in SpecialSym for char in passwd):
        return {'success': False, 'msg': 'Password should have at least one of the symbols $ @ # % , . ? !'}

    if val:
        return {'success': True, 'msg': 'password is valid'}
