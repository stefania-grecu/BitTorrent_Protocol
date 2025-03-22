#include <mpi.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define TRACKER_RANK 0
#define MAX_FILES 10
#define MAX_FILENAME 15
#define HASH_SIZE 32
#define MAX_CHUNKS 100

#define MAX_CLIENTS 100

// structura pentru un fisier
typedef struct {
    char filename[MAX_FILENAME];
    int num_chunks;
    char hashes[MAX_CHUNKS][HASH_SIZE + 1];
    int type;           // 0 - seed, 1 - peer, 2 - leecher pentru fisierele detinute
} StructFile;

// structura pentru un client
typedef struct {
    int num_files_i;
    StructFile files_i[MAX_FILES];
    int num_files_o;
    char files_o[MAX_FILES][MAX_FILENAME];
    // nu cred ca mi trebuie
    int type_o_files[MAX_FILES];    // 0 - seed, 1 - peer, 2 - leecher pentru fisierele pe care le doreste
} StructClient;

typedef struct {
    char filename[MAX_FILENAME];
    int rank[MAX_CLIENTS];
    int num_ranks;
} StructSwarm;

int swarm_size = 0;
StructSwarm swarm[MAX_FILES];
StructClient clients[MAX_CLIENTS];
// structura care contine informatii despre fisiere -- o detine trackerul
StructFile files[MAX_FILES];
int num_files = 0;


void file_input(StructClient *client, char *filename) {
    FILE *file = fopen(filename, "r");
    if (file == NULL) {
        printf("Eroare la deschiderea fisierului %s\n", filename);
        exit(-1);
    }

    fscanf(file, "%d", &client->num_files_i);
    for (int i = 0; i < client->num_files_i; i++) {
        fscanf(file, "%s", client->files_i[i].filename);
        fscanf(file, "%d", &client->files_i[i].num_chunks);
        for (int j = 0; j < client->files_i[i].num_chunks; j++) {
            fscanf(file, "%s", client->files_i[i].hashes[j]);
        }
        client->files_i[i].type = 0;
    }

    fscanf(file, "%d", &client->num_files_o);
    for (int i = 0; i < client->num_files_o; i++) {
        fscanf(file, "%s", client->files_o[i]);
        client->type_o_files[i] = 2;
    }

    // initializare tip client peer -- are atat fisierul cat si il doreste
    for (int i = 0; i < client->num_files_i; i++) {
        for (int j = 0; j < client->num_files_o; j++) {
            if (strcmp(client->files_i[i].filename, client->files_o[j]) == 0) {
                client->files_i[i].type = 1;
                break;
            }
        }
    }

    fclose(file);
}

void *download_thread_func(void *arg)
{
    int rank = *(int*) arg;
    StructClient *client = &clients[rank];
    MPI_Status status;
    char requested_file[MAX_FILENAME];
    char response[256];

    // conter pentru 10 segmente descarcate
    int count = 0;
    // char ack[4];

    for (int i = 0; i < client->num_files_o; i++) {
        if (count == 10) {
            // cerere de actuazlizare a swarm-ului
            MPI_Send("ACK", 4, MPI_CHAR, TRACKER_RANK, 0, MPI_COMM_WORLD);
            count = 0;
        }
        int find_file = 0;
        strcpy(requested_file, client->files_o[i]);
        // cerere swarm pentru fisier
        MPI_Send(requested_file, strlen(requested_file) + 1, MPI_CHAR, TRACKER_RANK, 1, MPI_COMM_WORLD);
        // primire swarm
        MPI_Recv(response, 256, MPI_CHAR, TRACKER_RANK, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
        // primeste nr de segmente si segmentele
        int num_chunks;
        MPI_Recv(&num_chunks, 5, MPI_INT, TRACKER_RANK, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
        char hashes[MAX_CHUNKS][HASH_SIZE + 1];
        for (int j = 0; j < num_chunks; j++) {
            MPI_Recv(hashes[j], HASH_SIZE + 1, MPI_CHAR, TRACKER_RANK, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
        }

        // creare fisier in care sa salvez segmentele
        char filename[30];
        sprintf(filename, "client%d_%s", rank, requested_file);
        FILE *file = fopen(filename, "w");
        if (file == NULL) {
            printf("Eroare la deschiderea fisierului %s\n", filename);
            exit(-1);
        }

        // cautare segmente ce lipsesc
        for (int j = 0; j < client->num_files_i; j++) {
            if (strcmp(client->files_i[j].filename, requested_file) == 0) {
                find_file = 1;
                for (int k = 0; k < num_chunks; k++) {
                    int find_chunk = 0;
                    for (int l = 0; l < client->files_i[j].num_chunks; l++) {
                        if (strcmp(client->files_i[j].hashes[l], hashes[k]) == 0) {
                            find_chunk = 1;
                            // scriu in fisier segmentul
                            fprintf(file, "%s\n", hashes[k]);
                            count++;
                            // printf("Client %d a descarcat segmentul %s din fisierul %s\n", rank, hashes[k], requested_file);
                            break;
                        }
                    }

                    if (!find_chunk) {
                        // cerere segment la peer din swarm
                        char *token;
                        int find_peer = 0;
                        char response_copy[256];
                        strcpy(response_copy, response);
                        token = strtok(response_copy, " ");
                        while (token != NULL && !find_peer) {
                            int peer_rank = atoi(token);
                            char nack[5];
                            MPI_Send(hashes[k], strlen(hashes[k]) + 1, MPI_CHAR, peer_rank, 1, MPI_COMM_WORLD);
                            MPI_Recv(nack, 5, MPI_CHAR, peer_rank, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
                            if (strcmp(nack, "NACK") != 0) {
                                // scriu in fisier segmentul
                                fprintf(file, "%s\n", hashes[k]);
                                count++;
                                find_peer = 1;
                                break;
                            }
                            token = strtok(NULL, " ");
                        }
                    }
                }
            }
        }

        if (!find_file) {
            // clientul nu detine niciun segment din fisier
            // cerere segmente la peeri din swarm
            for (int j = 0; j < num_chunks; j++) {
                char *token;
                int find_peer = 0;
                // copie la response pentru a nu afecta strtok
                char response_copy[256];
                strcpy(response_copy, response);
                token = strtok(response_copy, " ");
                while (token != NULL && !find_peer) {
                    int peer_rank = atoi(token);
                    char nack[5];
                    MPI_Send(hashes[j], strlen(hashes[j]) + 1, MPI_CHAR, peer_rank, 1, MPI_COMM_WORLD);
                    MPI_Recv(nack, 5, MPI_CHAR, peer_rank, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
                    if (strcmp(nack, "NACK") != 0) {
                        // scriu in fisier segmentul
                        fprintf(file, "%s\n", hashes[j]);
                        count++;
                        find_peer = 1;
                        break;
                    }
                    token = strtok(NULL, " ");
                }
            }
        }

        // marcare descarcare fisier finalizata prin trimiterea numelui fisierului
        MPI_Send(requested_file, strlen(requested_file) + 1, MPI_CHAR, TRACKER_RANK, 2, MPI_COMM_WORLD);

        if (find_file) {
            // marcare client ca seed pentru fisierul descarcat
            for (int j = 0; j < client->num_files_i; j++) {
                if (strcmp(client->files_i[j].filename, requested_file) == 0) {
                    client->files_i[j].type = 0;
                    break;
                }
            }
        }
    }
    // trimite mesaj ca a descarcat toate fisierele
    MPI_Send("ACK", 4, MPI_CHAR, TRACKER_RANK, 3, MPI_COMM_WORLD);
    
    return NULL;
}

void *upload_thread_func(void *arg)
{
    int rank = *(int*) arg;
    MPI_Status status;
    char buffer[256];
    StructClient *client = &clients[rank];

    // primire cereri de segmente
    while (1) {
        MPI_Recv(buffer, 256, MPI_CHAR, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);

        if (status.MPI_TAG == 1) {
            // client cere un segment
            char requested_segment[33];
            strcpy(requested_segment, buffer);

            int find_segment = 0;

            // cautare segment in fisierele detinute
            for (int i = 0; i < client->num_files_i; i++) {
                for (int j = 0; j < client->files_i[i].num_chunks; j++) {
                    if (strcmp(client->files_i[i].hashes[j], requested_segment) == 0) {
                        // trimite segmentul
                        find_segment = 1;
                        MPI_Send(client->files_i[i].hashes[j], strlen(client->files_i[i].hashes[j]) + 1, MPI_CHAR, status.MPI_SOURCE, 1, MPI_COMM_WORLD);
                    }
                }
            }
            if (!find_segment) {
                // segmentul nu a fost gasit
                MPI_Send("NACK", 5, MPI_CHAR, status.MPI_SOURCE, 1, MPI_COMM_WORLD);
            }
        } else if (status.MPI_TAG == 2) {
            // finalizare descarcare fisier
            return NULL;
            break;
        }
    }

    return NULL;
}

void tracker(int numtasks, int rank) {
    MPI_Status status;
    char buffer[256];
    // char ack[4];

    // primire mesaje de la clienti
    while (1) {
        MPI_Recv(buffer, 256, MPI_CHAR, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
        if (status.MPI_TAG == 0) {
            // primire rank client
            // int client_rank = atoi(buffer);

            // completare informatii despre fisierele detinute
            int num_files;
            MPI_Recv(&num_files, 1, MPI_INT, status.MPI_SOURCE, 0, MPI_COMM_WORLD, &status);

            for (int i = 0; i < num_files; i++) {
                StructFile file;
                MPI_Recv(file.filename, MAX_FILENAME, MPI_CHAR, status.MPI_SOURCE, 0, MPI_COMM_WORLD, &status);
                MPI_Recv(&file.num_chunks, 1, MPI_INT, status.MPI_SOURCE, 0, MPI_COMM_WORLD, &status);
                for (int j = 0; j < file.num_chunks; j++) {
                    MPI_Recv(file.hashes[j], HASH_SIZE + 1, MPI_CHAR, status.MPI_SOURCE, 0, MPI_COMM_WORLD, &status);
                }

                files[num_files] = file;
                num_files++;

                // adaugare fisier in swarm
                int find_file = 0;
                for (int j = 0; j < swarm_size; j++) {
                    if (strcmp(swarm[j].filename, file.filename) == 0) {
                        find_file = 1;
                        
                        int find_rank = 0;
                        for (int k = 0; k < swarm[j].num_ranks; k++) {
                            if (swarm[j].rank[k] == status.MPI_SOURCE) {
                                find_rank = 1;
                                break;
                            }
                        }

                        if (!find_rank) {
                            swarm[j].rank[swarm[j].num_ranks] = status.MPI_SOURCE;
                            swarm[j].num_ranks++;
                        }
                    }
                }

                if (!find_file) {
                    strcpy(swarm[swarm_size].filename, file.filename);
                    swarm[swarm_size].rank[0] = status.MPI_SOURCE;
                    swarm[swarm_size].num_ranks = 1;
                    swarm_size++;
                }
            }
            MPI_Send("ACK", 4, MPI_CHAR, status.MPI_SOURCE, 1, MPI_COMM_WORLD);
        } else if (status.MPI_TAG == 1) {
            // client cere swarm pentru un fisier
            char requested_file[MAX_FILENAME];
            strcpy(requested_file, buffer);

            int file_index = 0;

            char response[256] = "";
            for (int i = 0; i < swarm_size; i++) {
                if (strcmp(swarm[i].filename, requested_file) == 0) {
                    char entry[50];
                    sprintf(entry, "%d ", swarm[i].rank);
                    strcat(response, entry);
                    file_index = i;
                }
            }

            // marcheaza ca fisierul este detinut si de clientul care l-a cerut
            swarm[file_index].rank[swarm[file_index].num_ranks] = status.MPI_SOURCE;
            swarm[file_index].num_ranks++;

            // trimite swarm pentru fisier
            MPI_Send(response, strlen(response) + 1, MPI_CHAR, status.MPI_SOURCE, 1, MPI_COMM_WORLD);
            // trimite numar de segmente pentru fisier si segmentele
            for (int i = 0; i < num_files; i++) {
                if (strcmp(files[i].filename, requested_file) == 0) {
                    MPI_Send(&files[i].num_chunks, 5, MPI_INT, status.MPI_SOURCE, 0, MPI_COMM_WORLD);
                    for (int j = 0; j < files[i].num_chunks; j++) {
                        MPI_Send(files[i].hashes[j], strlen(files[i].hashes[j]) + 1, MPI_CHAR, status.MPI_SOURCE, 0, MPI_COMM_WORLD);
                    }
                }
            }

            
        } else if (status.MPI_TAG == 2) {
            // finalizare descarcare fisier
            // marcare client ca seed pentru fisierul descarcat
            int client_rank = status.MPI_SOURCE;
            char filename_downloaded[MAX_FILENAME];
            strcpy(filename_downloaded, buffer);

            for (int i = 0; i < swarm_size; i++) {
                if (strcmp(swarm[i].filename, filename_downloaded) == 0) {
                    for (int j = 0; j < swarm[i].num_ranks; j++) {
                        if (swarm[i].rank[j] == client_rank) {
                            // marcare client ca seed
                            for (int k = 0; k < clients[client_rank].num_files_i; k++) {
                                if (strcmp(clients[client_rank].files_i[k].filename, filename_downloaded) == 0) {
                                    clients[client_rank].files_i[k].type = 0;
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        } else if (status.MPI_TAG == 3) {
            // descarcare finalizata a tuturor fisierelor unui client
            // marcare client ca terminat
            int client_rank = status.MPI_SOURCE;
            for (int i = 0; i < clients[client_rank].num_files_o; i++) {
                clients[client_rank].type_o_files[i] = 0;
            }
        }
        else if (status.MPI_TAG == 4) {
            // toti clientii au terminat de descarcat toate fisierele
            // trackerul trimite mesaje la toti clientii sa se opreasca
            for (int i = 1; i < numtasks; i++) {
                MPI_Send("STOP", 5, MPI_CHAR, i, 2, MPI_COMM_WORLD);
            }
        } 
    }

}

void peer(int numtasks, int rank) {
    pthread_t download_thread;
    pthread_t upload_thread;
    void *status;
    int r;

    StructClient client;
    // construire fisier din care se citesc datele
    // in<rank>.txt
    char filename[20];
    sprintf(filename, "in%d.txt", rank);
    file_input(&client, filename);

    // conversie int la char
    char str[10];
    sprintf(str, "%d", rank);

    // trimitere mesaj de avertizare
    MPI_Send(str, strlen(str) + 1, MPI_CHAR, TRACKER_RANK, 0, MPI_COMM_WORLD);

    // trimitere informatii despre fisierele detinute catre tracker
    for (int i = 0; i < client.num_files_i; i++) {
        // nume fisier
        MPI_Send(client.files_i[i].filename, strlen(client.files_i[i].filename) + 1, MPI_CHAR, TRACKER_RANK, 0, MPI_COMM_WORLD);
        // numar segmente
        MPI_Send(client.files_i[i].num_chunks, 1, MPI_INT, TRACKER_RANK, 0, MPI_COMM_WORLD);
        // segmente
        for (int j = 0; j < client.files_i[i].num_chunks; j++) {
            MPI_Send(client.files_i[i].hashes[j], strlen(client.files_i[i].hashes[j]) + 1, MPI_CHAR, TRACKER_RANK, 0, MPI_COMM_WORLD);
        }
    }

    // afisare fisiere detinute
    printf("Client %d detine fisierele:\n", rank);
    for (int i = 0; i < client.num_files_i; i++) {
        printf("%s\n", client.files_i[i].filename);
    }


    // adaugare la clients
    clients[rank] = client;

    char ack[4];
    MPI_Recv(ack, 4, MPI_CHAR, TRACKER_RANK, MPI_ANY_TAG, MPI_COMM_WORLD, status);

    r = pthread_create(&download_thread, NULL, download_thread_func, (void *) &rank);
    if (r) {
        printf("Eroare la crearea thread-ului de download\n");
        exit(-1);
    }

    r = pthread_create(&upload_thread, NULL, upload_thread_func, (void *) &rank);
    if (r) {
        printf("Eroare la crearea thread-ului de upload\n");
        exit(-1);
    }

    r = pthread_join(download_thread, &status);
    if (r) {
        printf("Eroare la asteptarea thread-ului de download\n");
        exit(-1);
    }

    r = pthread_join(upload_thread, &status);
    if (r) {
        printf("Eroare la asteptarea thread-ului de upload\n");
        exit(-1);
    }
}
 
int main (int argc, char *argv[]) {
    int numtasks, rank;
 
    int provided;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
    if (provided < MPI_THREAD_MULTIPLE) {
        fprintf(stderr, "MPI nu are suport pentru multi-threading\n");
        exit(-1);
    }
    MPI_Comm_size(MPI_COMM_WORLD, &numtasks);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    if (rank == TRACKER_RANK) {
        tracker(numtasks, rank);
    } else {
        peer(numtasks, rank);
    }

    MPI_Finalize();
}
