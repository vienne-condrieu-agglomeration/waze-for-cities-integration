# Github Memo

## Create a new repository on the command line
```shell
$ echo "# waze-for-cities-integration" >> README.md
$ git init
$ git add README.md
$ git commit -m "first commit"
$ git branch -M main
$ git remote add origin https://github.com/vienne-condrieu-agglomeration/waze-for-cities-integration.git
$ git push -u origin main
```

## Push an existing repository from the command line
```shell
git remote add origin https://github.com/vienne-condrieu-agglomeration/waze-for-cities-integration.git
git branch -M main
git push -u origin main
```

## Configuration

Identity Name

```shell
$ git config --global user.name "fallamanche"
```

Identity Email

```shell
$ git config --global user.email "fallamanche@viennagglo.fr"
```

Editor Tool

```shell
$ git config --global core.editor subl
```

Diff Tool

```shell
$ git config --global merge.tool filemerge
```

Liste des globals

```shell
$ git config --list
```

## Principales commandes

Status des fichiers

```shell
$ git status
```

Lister les branchs

```shell
$ git branch
```

`*`sur la branche courante.

Créer une branch

```shell
$ git branch nom_de_ma_branch
```

Changer de branch

```shell
$ git checkout nom_de_ma_branch
```

Premier commit

```shell
$ git add .
$ git commit - m "initial commit"
```

Commit suivant

```shell
$ git add chemin_vers_mon_fichier
$ git commit -m "message du commit"
```

Annuler le dernier commit et modifs

```shell
$ git reset --hard md5_commit
$ git push --force
```

Mettre à jour le dépôt local

```shell
$ git pull
```

Envoyer ses commits vers le dépôt distant

```shell
$ git push
```

Supprimer un fichier du répertoire de travail et de l'index

```shell
$ git rm nom_du_fichier
```

Supprimer un fichier de l'index

```shell
$ git rmg --cached nom_du_fichier
```

## Diff

Affiche la différence entre le contenu du dernier commit et celui du répertoire de travail. Cela correspond à ce qui serait commité par git commit -a.

```shell
$ git diff HEAD
```

Affiche la différence entre le contenu pointé par A et celui pointé par B.

```shell
$ git diff A B
```

Diff entre un dossier présent sur deux branches

```shell
$ git diff master..MA_BRANCH chemin/vers/mon_dossier
```


## Log

```shell
$ git log
# Affiche X derniers commits
$ git log -n X
# Affiche un ensemble de commits par date
$ git log --since=date --until=date
```

Représentation de l’historique à partir de HEAD (commit / branch)

```shell
$ git log --oneline --graph --decorate
```

Représentation de l’historique à partir d'un fichier (commit / branch)

```shell
$ git log --oneline --graph --decorate nom_du_fichier
```

## Annuler commits (soft)

Seul le commit est retiré de Git ; vos fichiers, eux, restent modifiés. Vous pouvez alors à nouveau changer vos fichiers si besoin est et refaire un commit.

Annuler le dernier commit

```shell
$ git reset HEAD^
```

Pour indiquer à quel commit on souhaite revenir, il existe plusieurs notations :

* HEAD : dernier commit ;
* HEAD^ : avant-dernier commit ;
* HEAD^^ : avant-avant-dernier commit ;
* HEAD~2 : avant-avant-dernier commit (notation équivalente) ;
* d6d98923868578a7f38dea79833b56d0326fcba1 : indique un numéro de commit précis ;

## Annuler commits (hard)

Si vous voulez annuler votre dernier commit et les changements effectués dans les fichiers, il faut faire un reset hard. *Cela annulera sans confirmation tout votre travail !*

Annuler les commits et perdre tous les changements

```shell
$ git reset --hard HEAD^
```

*Annuler les modifications d’un fichier avant un commit*

Si vous avez modifié plusieurs fichiers mais que vous n’avez pas encore envoyé le commit et que vous voulez restaurer un fichier tel qu’il était au dernier commit :

```shell
$ git checkout nom_du_fichier
```

*Annuler/Supprimer un fichier avant un commit*

Supposer que vous venez d’ajouter un fichier à Git avec `git add` et que vous vous apprêtez à le « commiter ». Cependant, vous vous rendez compte que ce fichier est une mauvaise idée et vous voulez annuler votre `git add`.

Il est possible de retirer un fichier qui avait été ajouté pour être « commité » en procédant comme suit :

```shell
$ git reset HEAD -- nom_du_fichier_a_supprimer
```
