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
int num_files_all = 0;

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

    fclose(file);
}

void *download_thread_func(void *arg)
{
    int rank = *(int*) arg;
    // printf("Client %d\n", rank);
    StructClient *client = &clients[rank];
    MPI_Status status;
    char requested_file[MAX_FILENAME];
    char response[256];

    printf("Client %d are %d fisiere\n", rank, client->num_files_o);
    printf("Client %d are %d fisiere\n", rank, clients[rank].num_files_i);

    // conter pentru 10 segmente descarcate
    int count = 0;

    // printf("Client %d are %d fisiere\n", rank, client->num_files_o);

    for (int i = 0; i < client->num_files_o; i++) {
        // int find_file = 0;
        strcpy(requested_file, client->files_o[i]);
        // cerere swarm pentru fisier
        // printf("Client %d cere fisierul %s\n", rank, requested_file);
        MPI_Send(requested_file, strlen(requested_file) + 1, MPI_CHAR, TRACKER_RANK, 1, MPI_COMM_WORLD);

        printf("Client %d cere swarm pentru fisierul %s\n", rank, requested_file);

        // primire swarm "0 2 1 4"
        MPI_Recv(response, 256, MPI_CHAR, TRACKER_RANK, MPI_ANY_TAG, MPI_COMM_WORLD, &status);

        printf("swarmul este: %s\n", response);

        // calculare numar de  clienti ce detin fisierul
        int num_peers = 0;
        // vector de clienti ce detin fisierul
        int peers[MAX_CLIENTS];
        char *token;
        token = strtok(response, " ");
        while (token != NULL) {
            peers[num_peers] = atoi(token);
            num_peers++;
            token = strtok(NULL, " ");
        }

        int peer = rand() % num_peers;
        int count_segments = 0, all_segments;

        for (int j = 0; j < num_files_all; j++) {
            if (strcmp(files[j].filename, requested_file) == 0) {
                all_segments = files[j].num_chunks;
                break;
            }
        }

        printf("verificarile sunt ok\n");
        printf("all_segments: %d\n", all_segments);
        printf("num_files_all: %d\n", num_files_all);
        printf("filename: %s\n", requested_file);
        printf("num_peers: %d\n", num_peers);

        printf("FILESSSS: %s\n", files[0].filename);

        while (count_segments < all_segments) {
            char buffer[256];
            strcpy(buffer,"cerere ");
            strcat(buffer, requested_file);
            strcat(buffer, " ");
            // strcat(buffer, "%d", count_segments);
            char temp[10];
            sprintf(temp, "%d", count_segments);
            strcat(buffer, temp);
            printf("buffer download: %s\n", buffer);
            MPI_Send(buffer, strlen(buffer) + 1, MPI_CHAR, peers[peer], 0, MPI_COMM_WORLD);
            printf("send cerere %s\n", buffer);
            // primire segment
            char segment[33];
            MPI_Recv(segment, 33, MPI_CHAR, peers[peer], 1, MPI_COMM_WORLD, &status);

            // adaug fisierul in client
            count_segments++;
            
            int find_file = 0;
            for (int j = 0; j < client->num_files_i; j++) {
                if (strcmp(client->files_i[j].filename, requested_file) == 0) {
                    client->files_i[j].num_chunks = count_segments;
                    strcpy(client->files_i[j].hashes[count_segments], segment);
                    client->files_i[j].type = 1;
                    find_file = 1;
                    break;
                }
            }
            if (!find_file) {
                client->files_i[client->num_files_i].num_chunks = count_segments;
                strcpy(client->files_i[client->num_files_i].filename, requested_file);
                strcpy(client->files_i[client->num_files_i].hashes[count_segments], segment);
                client->files_i[client->num_files_i].type = 1;
                client->num_files_i++;
            }

            count++;

            if (count == 10) {
                count = 0;
                // trimitere mesaj ca a descarcat 10 segmente
                MPI_Send("ACK", 4, MPI_CHAR, TRACKER_RANK, 5, MPI_COMM_WORLD);
                MPI_Send(requested_file, strlen(requested_file) + 1, MPI_CHAR, TRACKER_RANK, 5, MPI_COMM_WORLD);
                MPI_Send(client, sizeof(StructClient), MPI_BYTE, TRACKER_RANK, 5, MPI_COMM_WORLD);

                // asteptare swarm actualizat
                MPI_Recv(response, 256, MPI_CHAR, TRACKER_RANK, 2, MPI_COMM_WORLD, &status);
            }
        }

        // creeare fisier output
        char filename[30];
        sprintf(filename, "client%d_%s", rank, requested_file);
        FILE *file = fopen(filename, "w");
        if (file == NULL) {
            printf("Eroare la deschiderea fisierului %s\n", filename);
            exit(-1);
        }

        for (int j = 0; j < client->num_files_i; j++) {
            if (strcmp(client->files_i[j].filename, requested_file) == 0) {
                for (int k = 0; k < client->files_i[j].num_chunks; k++) {
                    fprintf(file, "%s\n", client->files_i[j].hashes[k]);
                }
            }
        }

        MPI_Send(requested_file, strlen(requested_file) + 1, MPI_CHAR, TRACKER_RANK, 2, MPI_COMM_WORLD);

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
        MPI_Recv(buffer, 256, MPI_CHAR, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &status);
        // desfacere buffer
        char *token;
        char response[256];
        token = strtok(buffer, " ");
        char requested_file[MAX_FILENAME];
        char first[10];
        strcpy(first, token);

        printf("buffer: %s\n", buffer);
        
        if (strcmp(first, "cerere") == 0) {
            token = strtok(NULL, " ");
            strcpy(requested_file, token);
            token = strtok(NULL, " ");
            int segment = atoi(token);

            // cautare segment in fisierele detinute
            for (int i = 0; i < client->num_files_i; i++) {
                if (strcmp(client->files_i[i].filename, requested_file) == 0) {
                    // trimite segmentul
                    MPI_Send(client->files_i[i].hashes[segment], strlen(client->files_i[i].hashes[segment]) + 1, MPI_CHAR, status.MPI_SOURCE, 1, MPI_COMM_WORLD);
                }
            }
        } else if (strcmp(first, "STOP") == 0) {
            // oprire client
            return NULL;
        }
    }

    return NULL;
}

void tracker(int numtasks, int rank) {
    MPI_Status status;
    char buffer[256];
    char ack[4];
    int count_clients = numtasks - 1;
    // int num_clients = 0;

    while (1) {
        MPI_Recv(buffer, 256, MPI_CHAR, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);

        if (status.MPI_TAG == 0) {
            // completare informatii despre fisierele detinute
            int num_files;
            MPI_Recv(&num_files, 1, MPI_INT, status.MPI_SOURCE, 0, MPI_COMM_WORLD, &status);
            // num_clients++;

            for (int i = 0; i < num_files; i++) {
                StructFile file;
                MPI_Recv(file.filename, MAX_FILENAME, MPI_CHAR, status.MPI_SOURCE, 0, MPI_COMM_WORLD, &status);
                MPI_Recv(&file.num_chunks, 1, MPI_INT, status.MPI_SOURCE, 0, MPI_COMM_WORLD, &status);
                for (int j = 0; j < file.num_chunks; j++) {
                    MPI_Recv(file.hashes[j], HASH_SIZE + 1, MPI_CHAR, status.MPI_SOURCE, 0, MPI_COMM_WORLD, &status);
                }

                // afisare informatii despre fisier
                printf("Tracker a primit fisierul %s de la clientul %d\n", file.filename, status.MPI_SOURCE);
                printf("fisierul are %d segmente\n", file.num_chunks);

                // adaugare fisier in lista de fisiere
                int find_file_in_list = 0;
                for (int j = 0; j < num_files_all; j++) {
                    if (strcmp(files[j].filename, file.filename) == 0) {
                        find_file_in_list = 1;
                        break;
                    }
                }

                if (!find_file_in_list) {
                    strcpy(files[num_files_all].filename, file.filename);
                    files[num_files_all].num_chunks = file.num_chunks;
                    for (int j = 0; j < file.num_chunks; j++) {
                        strcpy(files[num_files_all].hashes[j], file.hashes[j]);
                    }
                    num_files_all++;
                }

                printf("Tracker a primit fisierul %s\n", files[num_files_all - 1].filename);
                printf("Tracker are %d fisiere\n", num_files_all);

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
            // if (num_clients == numtasks - 1) {
                for (int i = 1; i < numtasks; i++) {
                    MPI_Send("ACK", 4, MPI_CHAR, i, 0, MPI_COMM_WORLD);
                }
            // }
            // MPI_Send("ACK", 4, MPI_CHAR, status.MPI_SOURCE, 1, MPI_COMM_WORLD);

        } else 
        if (status.MPI_TAG == 1) {
            // client cere swarm pentru un fisier
            char requested_file[MAX_FILENAME];
            strcpy(requested_file, buffer);

            int file_index = 0;

            printf ("tracker: %s\n", requested_file);

            char response[256] = "";
            for (int i = 0; i < swarm_size; i++) {
                if (strcmp(swarm[i].filename, requested_file) == 0) {
                    char entry[50];
                    // sprintf(entry, "%d ", swarm[i].rank);
                    // strcat(response, entry);
                    file_index = i;
                    for (int j = 0; j < swarm[i].num_ranks; j++) {
                        sprintf(entry, "%d ", swarm[i].rank[j]);
                        strcat(response, entry);
                    }
                }
            }

            // trimite swarm pentru fisier
            // printf("Tracker trimite swarm pentru fisierul %s\n", requested_file);
            MPI_Send(response, strlen(response) + 1, MPI_CHAR, status.MPI_SOURCE, 1, MPI_COMM_WORLD);
            // // trimite numar de segmente pentru fisier si segmentele
            // for (int i = 0; i < num_files_all; i++) {
            //     if (strcmp(files[i].filename, requested_file) == 0) {
            //         MPI_Send(&files[i].num_chunks, 5, MPI_INT, status.MPI_SOURCE, 0, MPI_COMM_WORLD);
            //         for (int j = 0; j < files[i].num_chunks; j++) {
            //             MPI_Send(files[i].hashes[j], strlen(files[i].hashes[j]) + 1, MPI_CHAR, status.MPI_SOURCE, 0, MPI_COMM_WORLD);
            //         }
            //     }
            // }
            
            // marcheaza ca fisierul este detinut si de clientul care l-a cerut
            swarm[file_index].rank[swarm[file_index].num_ranks] = status.MPI_SOURCE;
            swarm[file_index].num_ranks++;

        } else if (status.MPI_TAG == 2) {
            // finalizare descarcare fisier
            // marcare client ca seed
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
            count_clients--;
            if (count_clients == 0) {
                // toti clientii au terminat de descarcat
                // trackerul trimite mesaje la toti clientii sa se opreasca
                for (int i = 1; i < numtasks; i++) {
                    MPI_Send("STOP", 5, MPI_CHAR, i, 0, MPI_COMM_WORLD);
                }
            }
            
        // }
        // else if (status.MPI_TAG == 4) {
        //     // toti clientii au terminat de descarcat toate fisierele
        //     // trackerul trimite mesaje la toti clientii sa se opreasca
        //     for (int i = 1; i < numtasks; i++) {
        //         MPI_Send("STOP", 5, MPI_CHAR, i, 2, MPI_COMM_WORLD);
        //     }
        } else if (status.MPI_TAG == 5) {
            // nume fisier
            MPI_Recv(buffer, 256, MPI_CHAR, status.MPI_SOURCE, 5, MPI_COMM_WORLD, &status);
            // client
            StructClient client;
            MPI_Recv(&client, sizeof(StructClient), MPI_BYTE, status.MPI_SOURCE, 5, MPI_COMM_WORLD, &status);
            int swarm_index = 0;
            for (int i = 0; i < swarm_size; i++) {
                if (strcmp(swarm[i].filename, buffer) == 0) {
                    swarm_index = i;
                    int find_rank = 0;
                    for (int j = 0; j < swarm[i].num_ranks; j++) {
                        if (swarm[i].rank[j] == status.MPI_SOURCE) {
                            // marcare client ca seed
                            find_rank = 1;
                            for (int k = 0; k < client.num_files_i; k++) {
                                if (strcmp(client.files_i[k].filename, buffer) == 0) {
                                    client.files_i[k].type = 0;
                                    break;
                                }
                            }
                        }
                    }
                    if (!find_rank) {
                        swarm[i].rank[swarm[i].num_ranks] = status.MPI_SOURCE;
                        swarm[i].num_ranks++;
                    }
                }
            }

            // trimitere swarm actualizat
            char swarm_actualizat[256];
            strcpy(swarm_actualizat, "");
            for (int i = 0; i < swarm[swarm_index].num_ranks; i++) {
                char entry[50];
                sprintf(entry, "%d ", swarm[swarm_index].rank[i]);
                strcat(swarm_actualizat, entry);
            }
            MPI_Send(swarm_actualizat, strlen(swarm_actualizat) + 1, MPI_CHAR, status.MPI_SOURCE, 2, MPI_COMM_WORLD);

        }
        if (count_clients == 0) {
            break;
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
    printf("send1\n");

    //trimitere nr de fisisere detinute
    MPI_Send(&client.num_files_i, 1, MPI_INT, TRACKER_RANK, 0, MPI_COMM_WORLD);
    printf("send2\n");

    // trimitere informatii despre fisierele detinute catre tracker
    for (int i = 0; i < client.num_files_i; i++) {
        // nume fisier
        MPI_Send(client.files_i[i].filename, strlen(client.files_i[i].filename) + 1, MPI_CHAR, TRACKER_RANK, 0, MPI_COMM_WORLD);
        // numar segmente
        MPI_Send(&client.files_i[i].num_chunks, 1, MPI_INT, TRACKER_RANK, 0, MPI_COMM_WORLD);
        // segmente
        for (int j = 0; j < client.files_i[i].num_chunks; j++) {
            MPI_Send(client.files_i[i].hashes[j], strlen(client.files_i[i].hashes[j]) + 1, MPI_CHAR, TRACKER_RANK, 0, MPI_COMM_WORLD);
        }
    }
    printf("send3\n");

    // adaugare la clients
    clients[rank] = client;

    char ack[4];
    MPI_Recv(ack, 4, MPI_CHAR, TRACKER_RANK, MPI_ANY_TAG, MPI_COMM_WORLD, status);
    // MPI_Send("ACK", 4, MPI_CHAR, TRACKER_RANK, 0, MPI_COMM_WORLD);
    // printf("Client %d a primit ack\n", rank);


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