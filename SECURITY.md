# 보안 정책 / Security policy

보안 취약점을 발견하셨다면 **공개 이슈 대신** 이 레포의 **Security → Report a vulnerability**(비공개 취약점 신고)로 알려 주세요. 확인 후 답변드리고, 고친 버전을 릴리스하면서 함께 안내합니다.

If you find a security issue, please **do not open a public issue**. Use **Security → Report a vulnerability** in this repository to report it privately. We will reply after checking and announce the fix with a release.

이 패키지는 라이브러리이며 네트워크 포트를 열지 않습니다. Redis 비밀번호는 코드가 아니라 `REDIS_URL` 이나 자체 설정에 두세요. `forSession` 으로 나눈 이벤트와 `SessionStore` 기록에는 세션 데이터가 들어 있으니 Redis 와 SQLite 파일을 보호하세요.

This package is a library and opens no network port. Keep your Redis password in `REDIS_URL` or your own configuration, not in code. Events scoped with `forSession` and `SessionStore` records contain session data, so protect your Redis server and SQLite file.
