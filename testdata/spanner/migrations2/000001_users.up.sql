CREATE TABLE Users2 (
  Id STRING(MAX),
  Username STRING(MAX),
  Firstname STRING(MAX),
  Lastname STRING(MAX),
  PasswordHash STRING(MAX),
  Email STRING(MAX),
) PRIMARY KEY(Id);

CREATE UNIQUE INDEX Users2_Username ON Users2(Username);