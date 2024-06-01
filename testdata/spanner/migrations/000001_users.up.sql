CREATE TABLE Users (
  Id STRING(MAX),
  Username STRING(MAX),
  Firstname STRING(MAX),
  Lastname STRING(MAX),
  PasswordHash STRING(MAX),
  Email STRING(MAX),
) PRIMARY KEY(Id);

CREATE UNIQUE INDEX Users_Username ON Users(Username);