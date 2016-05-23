#include <mpi.h>
#include <stdlib.h>
#include <stdio.h>
#include <time.h>
#include <stdarg.h>

#define MAX 1024.0f
#define NB_VOISINS 10 //nombre maximum de voisins que connait un proc
#define NB_VALUES 100 //le nombre de valeurs à inserer dans le programme test
#define NB_STOCK_VALUES 10 //le nombre de valeurs que stocke un proc
#define LOGFILE_INSERTION_NOEUD	"insertion_noeud.log" //le nom du fichier log
#define LOGFILE_INSERTION_DONNEE "insertion_donnee.log" //le nom du fichier log
#define LOGFILE_LECTURE_DONNEE "lecture_donnee.log" //le nom du fichier log
#define LOGFILE_SUPPRESSION_DONNEE "suppression_donnee.log" //le nom du fichier log

#define etiquette_invitation 10
#define etiquette_accepte_invit 11
#define etiquette_demande_insertion 12
#define etiquette_fin_insertion 13
#define etiquette_ajout_donnees 14
#define etiquette_fin_ajout 15
#define etiquette_cherche_donnees 16
#define etiquette_fin_recherche 17
#define etiquette_supprime_donnees 18
#define etiquette_fin_suppression 19
#define etiquette_fin_prg 20

MPI_Datatype mpi_coord, mpi_zone, mpi_attrib, mpi_donnee;
int LogCreated;
char* LogFile;

typedef struct struct_coordonnees{
	float lig, col;
}coordonnees;

typedef struct struct_zone{
	coordonnees x1, x2;
}zone;

typedef struct struct_voisins{
	int pid_voisin;
	struct struct_voisins * voisin_suivant;
}voisin;

typedef struct struct_donnee{
	float value;
	coordonnees coord;
}donnee;

typedef struct struct_attributs{
	int rang, nb_voisins, nb_values;
	donnee data[NB_STOCK_VALUES];
	int  pidvoisins[NB_VOISINS];
	zone zoneVoisines[NB_VOISINS];
	zone initiale, actuelle;
	coordonnees p_coord; //coordonnées du proc
}attributs;

int Log (char *str,...)
{
	FILE *file;
 
	if (!LogCreated) {
		file = fopen(LogFile, "w");
		LogCreated = 1;
	}
	else		
		file = fopen(LogFile, "a");
		
	if (file == NULL) {
		if (LogCreated)
			LogCreated = 0;
		return -1;
	}
	else
	{
		va_list arglist;
		va_start(arglist,str);
		vfprintf(file,str,arglist);
		va_end(arglist);

		fclose(file);
		return 1;
	}
}

void writeLog(attributs att) {
	int j;
	Log("%03d || [%.3f-%-.3f ; %.3f-%-.3f] || ", att.rang, att.actuelle.x1.col, att.actuelle.x1.lig,
	att.actuelle.x2.col, att.actuelle.x2.lig);
	for(j = 0; j < att.nb_values; j++) {
		Log("%.3f ", att.data[j].value);
	}
	Log("\n");
}

int min(float X, float Y){
	 return (((X) < (Y)) ? (X) : (Y));
}

int max(float X, float Y){
	 return (((X) > (Y)) ? (X) : (Y));
}

int appartient(coordonnees c, zone z){
	if(c.lig <= max(z.x1.lig, z.x2.lig) && c.lig >= min(z.x1.lig, z.x2.lig) && c.col <= max(z.x1.col, z.x2.col) && c.col >= min(z.x1.col, z.x2.col)){
		return 1;
	}else{
		return 0;
	}
}

float RandomFloat(float a, float b) {
    float random = ((float) rand()) / (float) RAND_MAX;
    float diff = b - a;
    float r = random * diff;
    return a + r;
}

coordonnees getRandCoordFromZone(zone z) {
     float maxX, maxY, minX, minY;
     coordonnees coord;
     maxX = max(z.x1.col, z.x2.col);
     minX = min(z.x1.col, z.x2.col);
     maxY = max(z.x1.lig, z.x2.lig);
     minY = min(z.x1.lig, z.x2.lig);
     coord.lig = RandomFloat(minY, maxY);
     coord.col = RandomFloat(minX, maxX);
     return coord;
}

int getClosestNeighbor(zone z[NB_VOISINS], coordonnees c) {
	int i;
	for(i = 0; i < NB_VOISINS; i++) {
		if(appartient(c, z[i]))
			return i;
	}
	return -1;
}

void insertion(attributs * entrant, attributs * proprietaire){
	zone z1, z2;
	int i, j;
	//etape 1, on verifie si coord_entrant appartient à zone totale
	if(appartient(entrant->p_coord, proprietaire->initiale)){
		//puis que le noeud est dans la sous zone du noeud proprietaire
		if(appartient(entrant->p_coord, proprietaire->actuelle)){
			//on recalcule note zone actuelle et la zone totale+actuelle de entrant
			z1.x1 = proprietaire->actuelle.x1;
			z2.x2 = proprietaire->actuelle.x2;
			if(proprietaire->actuelle.x2.col-proprietaire->actuelle.x1.col >= proprietaire->actuelle.x2.col-proprietaire->actuelle.x1.col){
				//on coupe en abcisse (numero de colonnes)
				z1.x2.lig = proprietaire->actuelle.x2.lig;
				z1.x2.col = z2.x1.col = ((proprietaire->actuelle.x2.col-proprietaire->actuelle.x1.col)/2)+proprietaire->actuelle.x1.col;
				z2.x1.lig = proprietaire->actuelle.x1.lig;
			}else{
				//on coupe en ordonnées (numero de lignes)
				z1.x2.col = proprietaire->actuelle.x2.col;
				z1.x2.lig = z2.x1.lig = ((proprietaire->actuelle.x2.lig-proprietaire->actuelle.x1.lig)/2)+proprietaire->actuelle.x1.lig;
				z2.x1.col = proprietaire->actuelle.x1.col;
			}
			//on affecte les zones à nos proc
			if(appartient(proprietaire->p_coord, z1)){
				entrant->initiale = entrant->actuelle = z2;
				proprietaire->actuelle = z1;
			}else{
				entrant->initiale = entrant->actuelle = z1;
				proprietaire->actuelle = z2;
			}
			
			//on s'ajoute un voisin
			proprietaire->pidvoisins[proprietaire->nb_voisins] = entrant->rang;
			proprietaire->zoneVoisines[proprietaire->nb_voisins] = entrant->initiale;
			(proprietaire->nb_voisins)++;
			
			//on verifie que l'entrant obtient une zone contenant son identifiant
			if(! appartient(entrant->p_coord, entrant->actuelle)){
				entrant->p_coord = getRandCoordFromZone(entrant->actuelle);
			}			
			//on lui envoi un message pour lui dire que tout va bien avec sa zone
			MPI_Send(entrant, 1, mpi_attrib, entrant->rang, etiquette_fin_insertion, MPI_COMM_WORLD);
			
		}else{
			j = getClosestNeighbor(proprietaire->zoneVoisines, entrant->p_coord);
			MPI_Send(entrant, 1, mpi_attrib, (proprietaire->pidvoisins)[j], etiquette_demande_insertion, MPI_COMM_WORLD);
		}
	}
	//Sinon, cas d'arret, on ne fait rien
}

void ajout_donnees(attributs * att, donnee ddaattaa){
	int j;
	//etape 1, on verifie si coord_entrant appartient à zone totale
	if(appartient(ddaattaa.coord, att->initiale)){
		//puis que le noeud est dans la sous zone du noeud proprietaire
		if(appartient(ddaattaa.coord, att->actuelle)){
			//ajout données 
			if((att->nb_values)<NB_STOCK_VALUES){
				(att->data)[att->nb_values] = ddaattaa;
				(att->nb_values)++;
			}
			MPI_Send(att, 1, mpi_attrib, 0, etiquette_fin_ajout, MPI_COMM_WORLD);
			writeLog(*att);
		}else{
			j = getClosestNeighbor(att->zoneVoisines, ddaattaa.coord);
			MPI_Send(&ddaattaa, 1, mpi_donnee, (att->pidvoisins)[j], etiquette_ajout_donnees, MPI_COMM_WORLD);
		}
	}
}

void cherche_donnees(attributs * att, coordonnees * coord){
	int j;
	donnee data;
	//etape 1, on verifie si coord_entrant appartient à zone totale
	if(appartient(*coord, att->initiale)){
		//puis que le noeud est dans la sous zone du noeud proprietaire
		if(appartient(*coord, att->actuelle)){
			data.value = 0.;
			for(j=0;j<att->nb_values;j++){
				if(att->data[j].coord.col==coord->col && att->data[j].coord.col==coord->col){
					data = att->data[j];
					break;
				}
			}
			MPI_Send(&data, 1, mpi_donnee, 0, etiquette_fin_recherche, MPI_COMM_WORLD);
			writeLog(*att);
		}else{
			j = getClosestNeighbor(att->zoneVoisines, *coord);
			MPI_Send(coord, 1, mpi_coord, (att->pidvoisins)[j], etiquette_cherche_donnees, MPI_COMM_WORLD);
		}
	}
}

void supprime_donnees(attributs * att, coordonnees * coord){
	int j, cpt;
	donnee data;
	//etape 1, on verifie si coord_entrant appartient à zone totale
	if(appartient(*coord, att->initiale)){
		//puis que le noeud est dans la sous zone du noeud proprietaire
		if(appartient(*coord, att->actuelle)){
			for(j=0;j<att->nb_values;j++){
				if(att->data[j].coord.col==coord->col && att->data[j].coord.col==coord->col){
					//On supprime J
					for(cpt = j; cpt < (att->nb_values - 1); cpt++){
						att->data[cpt] = att->data[cpt+1];
					}
					(att->nb_values)--;
					break;
				}
			}
			MPI_Send(NULL, 0, MPI_INT, 0, etiquette_fin_suppression, MPI_COMM_WORLD);
			writeLog(*att);
		}else{
			j = getClosestNeighbor(att->zoneVoisines, *coord);
			MPI_Send(coord, 1, mpi_coord, (att->pidvoisins)[j], etiquette_supprime_donnees, MPI_COMM_WORLD);
		}
	}
}

int main(int argc, char *argv[]) {
	
        int rang, nb_procs, i;
    
        /* Initialisation de MPI */
        MPI_Init(&argc, &argv);
        MPI_Comm_rank(MPI_COMM_WORLD, &rang);
        MPI_Comm_size(MPI_COMM_WORLD, &nb_procs);
        MPI_Status status;
        
        if(nb_procs < 2){
			printf("Il faut au minimum 2 processus : pour le coordinateur et le noeud de bootstrap.\n");
			return EXIT_FAILURE;
		}
        
        srand(time(NULL) + rang);//random offset rang
        
        
        /* Construction du type coordonnees dans MPI */
        MPI_Type_create_struct(2, (int[]){1,1}, (MPI_Aint[]){offsetof(coordonnees, lig), offsetof(coordonnees, col)}, (MPI_Datatype[]){MPI_FLOAT, MPI_FLOAT}, &mpi_coord);
        MPI_Type_commit(&mpi_coord);
        
        /* Construction du type zone dans MPI */
        MPI_Type_create_struct(2, (int[]){1,1}, (MPI_Aint[]){offsetof(zone, x1), offsetof(zone, x2)}, (MPI_Datatype[]){mpi_coord, mpi_coord}, &mpi_zone);
        MPI_Type_commit(&mpi_zone);
        
        /* Construction du type donnée dans MPI */
        MPI_Type_create_struct(2, (int[]){1,1}, (MPI_Aint[]){offsetof(donnee, value), offsetof(donnee, coord)}, (MPI_Datatype[]){MPI_FLOAT, mpi_coord}, &mpi_donnee);
        MPI_Type_commit(&mpi_donnee);
        
        /* Construction du type attribut dans MPI */
        MPI_Type_create_struct(9, (int[]){1,1,1,NB_STOCK_VALUES,NB_VOISINS,NB_VOISINS,1,1,1}, (MPI_Aint[]){offsetof(attributs, rang), offsetof(attributs, nb_voisins), offsetof(attributs, nb_values), offsetof(attributs, data), offsetof(attributs, pidvoisins), offsetof(attributs, zoneVoisines), offsetof(attributs, initiale), offsetof(attributs, actuelle), offsetof(attributs, p_coord)}, (MPI_Datatype[]){MPI_INT, MPI_INT, MPI_INT, mpi_donnee, MPI_INT, mpi_zone, mpi_zone, mpi_zone, mpi_coord}, &mpi_attrib);
        MPI_Type_commit(&mpi_attrib);
        
        //Chaque processus autre que le coordinateur tire une coordonnée
        if(rang==0){
			/******************************************************************************************************
			/***************Coordinateur***************************************************************************
			/*****************************************************************************************************/
			//Variables temporaires
			int j;
			int nb_insertions_randoms = 0;
			attributs att;
			donnee nouvelle_donnee;
			
			coordonnees tab_premiers[5] = {{0.,0.}, {0.,0.}, {0.,0.}, {0.,0.}, {0.,0.}};
			coordonnees tab_derniers[5] = {{0.,0.}, {0.,0.}, {0.,0.}, {0.,0.}, {0.,0.}};
			coordonnees tab_rands[5] = {{0.,0.}, {0.,0.}, {0.,0.}, {0.,0.}, {0.,0.}};
			
			//Insertion des noeuds
			LogCreated = 0;
			LogFile = LOGFILE_INSERTION_NOEUD;
			Log("Insertion des noeuds\n");
			for(i=1; i<nb_procs; i++){
				//On invite le processus i
				MPI_Send(NULL, 0, MPI_INTEGER, i, etiquette_invitation, MPI_COMM_WORLD);
				MPI_Recv(&att, 1, mpi_attrib, i, etiquette_accepte_invit, MPI_COMM_WORLD, &status);
				printf("proc %d s'est correctement inséré dans l'overlay\n", att.rang);
				writeLog(att);
			}
			
			//Insertion des données
			LogCreated = 0;
			LogFile = LOGFILE_INSERTION_DONNEE;
			Log("Insertion des données\n");
			for(i=0; i<NB_VALUES; i++){
				//On tire une valeur
				nouvelle_donnee.coord.col = RandomFloat(0, MAX);
				nouvelle_donnee.coord.lig = RandomFloat(0, MAX);
				nouvelle_donnee.value = (nouvelle_donnee.coord.col) + (nouvelle_donnee.coord.lig);
				MPI_Send(&nouvelle_donnee, 1, mpi_donnee, 1, etiquette_ajout_donnees, MPI_COMM_WORLD);
				MPI_Recv(&att, 1, mpi_attrib, MPI_ANY_SOURCE, etiquette_fin_ajout, MPI_COMM_WORLD, &status);
				printf("Ajout de la donnée %f à [%.3f;%.3f] par %d\n", nouvelle_donnee.value, nouvelle_donnee.coord.col, nouvelle_donnee.coord.lig, status.MPI_SOURCE);
				//on conserve les 5 premieres coordonnées
				if(i<5){
					tab_premiers[i] = nouvelle_donnee.coord;
				}
				//on conserve les 5 dernieres coordonnées
				for(j=4;j>0;j--){
					tab_derniers[j] = tab_derniers[j-1];
				}
				tab_derniers[0] = nouvelle_donnee.coord;
				if(nb_insertions_randoms < 5 && rand()%100 < 7){//7% de chances
					tab_rands[nb_insertions_randoms] = nouvelle_donnee.coord;
					nb_insertions_randoms++;
				}
			}
			
			//Recherche et lecture de données
			LogCreated = 0;
			LogFile = LOGFILE_LECTURE_DONNEE;
			Log("Recherche et lecture de données\n");
			for(j=0; j<5; j++){
				MPI_Send(&(tab_premiers[j]), 1, mpi_coord, 1, etiquette_cherche_donnees, MPI_COMM_WORLD);
				MPI_Recv(&nouvelle_donnee, 1, mpi_donnee, MPI_ANY_SOURCE, etiquette_fin_recherche, MPI_COMM_WORLD, &status);
				printf("On lit la donnée %f à l'adresse [%.3f;%.3f] gérée par %d\n", nouvelle_donnee.value, tab_premiers[j].col, tab_premiers[j].lig, status.MPI_SOURCE);
			}
			for(j=0; j<5; j++){
				MPI_Send(&(tab_derniers[j]), 1, mpi_coord, 1, etiquette_cherche_donnees, MPI_COMM_WORLD);
				MPI_Recv(&nouvelle_donnee, 1, mpi_donnee, MPI_ANY_SOURCE, etiquette_fin_recherche, MPI_COMM_WORLD, &status);
				printf("On lit la donnée %f à l'adresse [%.3f;%.3f] gérée par %d\n", nouvelle_donnee.value, tab_derniers[j].col, tab_derniers[j].lig, status.MPI_SOURCE);
			}
			for(j=0; j<5; j++){
				if(!tab_rands[j].col && !tab_rands[j].lig) break;
				MPI_Send(&(tab_rands[j]), 1, mpi_coord, 1, etiquette_cherche_donnees, MPI_COMM_WORLD);
				MPI_Recv(&nouvelle_donnee, 1, mpi_donnee, MPI_ANY_SOURCE, etiquette_fin_recherche, MPI_COMM_WORLD, &status);
				printf("On lit la donnée %f à l'adresse [%.3f;%.3f] gérée par %d\n", nouvelle_donnee.value, tab_rands[j].col, tab_rands[j].lig, status.MPI_SOURCE);
			}
			
			//Suppression de données
			LogCreated = 0;
			LogFile = LOGFILE_SUPPRESSION_DONNEE;
			Log("Suppression de %d donnée(s)\n", nb_insertions_randoms);
			for(j=0; j<5; j++){
				if(!tab_rands[j].col && !tab_rands[j].lig) break;
				MPI_Send(&(tab_rands[j]), 1, mpi_coord, 1, etiquette_supprime_donnees, MPI_COMM_WORLD);
				MPI_Recv(NULL, 0, MPI_INT, MPI_ANY_SOURCE, etiquette_fin_suppression, MPI_COMM_WORLD, &status);
				printf("Suppression OK\n");
			}
			
			//On verifie que les données sont effacées
			printf("Vérification de la suppression de(s) donnée(s)...\n");
			for(j=0; j<5; j++){
				if(!tab_rands[j].col && !tab_rands[j].lig) break;
				MPI_Send(&(tab_rands[j]), 1, mpi_coord, 1, etiquette_cherche_donnees, MPI_COMM_WORLD);
				MPI_Recv(&nouvelle_donnee, 1, mpi_donnee, MPI_ANY_SOURCE, etiquette_fin_recherche, MPI_COMM_WORLD, &status);
				printf("On lit la donnée %f à l'adresse [%.3f;%.3f] gérée par %d\n", nouvelle_donnee.value, tab_rands[j].col, tab_rands[j].lig, status.MPI_SOURCE);
			}
			
			//On notifie les autres processus, la fin de l'execution du test
			for(i=1; i<nb_procs; i++){
				MPI_Send(NULL, 0, MPI_INT, i, etiquette_fin_prg, MPI_COMM_WORLD);
			}
			
		}else{	
			/******************************************************************************************************
			/***************Autres*********************************************************************************
			/*****************************************************************************************************/

			attributs att, att_tmp;
			donnee donnee_tmp;
			coordonnees coord_tmp;
			
			//On recoit une invitation du coordinateur
			MPI_Recv(NULL, 0, MPI_INTEGER, 0, etiquette_invitation, MPI_COMM_WORLD, &status);
			
			//On tire des coordonnées		
			att.rang = rang;
			att.p_coord.lig = RandomFloat(0, MAX);
			att.p_coord.col = RandomFloat(0, MAX);
			
			att.nb_voisins = 0;
			att.nb_values = 0;
			
			if(rang==1){//noeud de bootstrap
				att.initiale.x1.col = 0.0f;
				att.initiale.x1.lig = 0.0f;
				att.initiale.x2.col = MAX;
				att.initiale.x2.lig = MAX;
				att.actuelle.x1.col = 0.0f;
				att.actuelle.x1.lig = 0.0f;
				att.actuelle.x2.col = MAX;
				att.actuelle.x2.lig = MAX;
			}else{
				//On s'insere dans l'overlay
				MPI_Send(&att, 1, mpi_attrib, 1, etiquette_demande_insertion, MPI_COMM_WORLD);
				
				//On attend la réponse du noeud propriétaire de la zone contenant la coordonnées
				MPI_Recv(&att, 1, mpi_attrib, MPI_ANY_SOURCE, etiquette_fin_insertion, MPI_COMM_WORLD, &status);
			}
			
			//On notifie le coordinateur
			MPI_Send(&att, 1, mpi_attrib, 0, etiquette_accepte_invit, MPI_COMM_WORLD);  

			for(;;){
				
				MPI_Probe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
				
				//Insertion de noeuds
				if(status.MPI_TAG == etiquette_demande_insertion){
					LogCreated = 1;
					LogFile = LOGFILE_INSERTION_NOEUD;
					MPI_Recv(&att_tmp, 1, mpi_attrib, MPI_ANY_SOURCE, etiquette_demande_insertion, MPI_COMM_WORLD, &status);
					insertion(&att_tmp, &att);
				}
				
				//Insertion de données
				if(status.MPI_TAG == etiquette_ajout_donnees){
					LogCreated = 1;
					LogFile = LOGFILE_INSERTION_DONNEE;
					MPI_Recv(&donnee_tmp, 1, mpi_donnee, MPI_ANY_SOURCE, etiquette_ajout_donnees, MPI_COMM_WORLD, &status);
					ajout_donnees(&att, donnee_tmp);
				}
				
				//Recherche de données
				if(status.MPI_TAG == etiquette_cherche_donnees){
					LogCreated = 1;
					LogFile = LOGFILE_LECTURE_DONNEE;
					MPI_Recv(&coord_tmp, 1, mpi_coord, MPI_ANY_SOURCE, etiquette_cherche_donnees, MPI_COMM_WORLD, &status);
					cherche_donnees(&att, &coord_tmp);
				}
				
				//Suppression de données
				if(status.MPI_TAG == etiquette_supprime_donnees){
					LogCreated = 1;
					LogFile = LOGFILE_SUPPRESSION_DONNEE;
					MPI_Recv(&coord_tmp, 1, mpi_coord, MPI_ANY_SOURCE, etiquette_supprime_donnees, MPI_COMM_WORLD, &status);
					supprime_donnees(&att, &coord_tmp);
				}
				
				//Fin execution
				if(status.MPI_TAG == etiquette_fin_prg){
					break;
				}

			}
			   
		}
        
        /* Fin de MPI */
        MPI_Type_free(&mpi_coord);
        MPI_Type_free(&mpi_zone);
        MPI_Type_free(&mpi_attrib);
        MPI_Finalize();
        
        return EXIT_SUCCESS;
}
