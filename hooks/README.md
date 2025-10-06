# Git hooks readme for developers

Fully flushed out syntax for commit is:

`[#IssueNumberOnGithub] type(optional scope input): any message`

For example ticket number 10, with type test, and scope api with the commit message being 'added api tests', your commit would be:

`[#10] test(api): added api tests`

If you find ticket numbers annoying or you don't have one, you should make one and still ticket. However, I'm not going to force you to make complicated commits, so at a minimum you can choose to do something easier such as:

`test(api): added api tests`

If you feel this is still too complicated, you can simplify even more too:

`test: added api tests`

List of commit types you can have (example above uses test): 

+ build
+ chore
+ ci
+ docs
+ feat
+ fix
+ git
+ perf
+ refactor
+ revert
+ style
+ test

### Developing commit hooks

there are currently text files in this repository. Those do not get used. The files that get used do not have any file extension and match the name of a file in the .git/hooks/ directory for what part of the version control process you would want to hook into.