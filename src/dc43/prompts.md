je voudrais creer une librairie python qui serait utilisee par defaut dans des scripts python databricks utilisant soit les api classiques de load et write de databricks mais aussi DLT si c'est possible (sinon ce serait une lib differente).
Le but est de generer, sotcker, faire evoluer, et utiliser des data contracts suivant le standard bitol.io directement dans les pipelines data.
idealement, en automatisant des etapes en utilisant des primitives de lectures et ecritures de delta tables

=================

oui fait ce que tu proposes, et j'aimerais qu'on utilise bitol v3.0.2 (si il y a une lib python de ref, on peut l'utiliser)
Dans le schema que j'ai mis il y a une delegation du travail de verification des contraintes data a un outil de DQ/DO, ce serait bien que la lib fasse cela... il faut une interface qui permette d'interoger un truc externe, ou un stub local comme ca on minimise les resp donnees a la lib de lecture ecriture directe, et un outil dedie pour le reste des checks
En gros a la lecture, le schema peut etre checke par rapport au contrat (avec la version attendue!) puis recevoir de l'oiutil de DQ si le dataset n'a pas de point bloquant connu (au vu des contraintes), sinon si le dataset evolue depuis sa derniere utilisation (il faut enregistrer les dates... idealement le dataset a une version qui est connue de l'outil de DQ et cette version evolue a chaque modif du dataset et et envoyee au DQ lorsque le dataset quality doit etre reevalue... si la version est plus recente du dataset que celui connu par le DQ alors la lib calcule les metriques necessaires pour le DQ pour faire ses validations et retourner (en le mettant a jour => suivi du status par version de dataset et par version de contrat forcement) le nouveau statut.
Par ailleurs, il doit y avoir une detection si la lib ne demande pas une version du data contract quoi soit inadequate pour le dataset, par exemple, si la version du dataset est connue et liee a une version de contrat particuliere

=================

j'aimerais que ce soit au plus simple, sans trop de concepts ou on se perd
donc le module de lib odcs (bitol, voir plus bas), le module integration avec spark/databriks, le stockage (fichier mais aussi delta), le module de communication avec la data quality (reception du statut actuel et celui d'envoi des metriques necessaires), et donc le module qui calcule les metriques (ou les resultats des checks si pas possible d'envoyer des metriques a un module car il faut envoyer trop ou tout)

Utilise la librairie https://pypi.org/project/open-data-contract-standard/ a la place de tout code inutile stub et autre.