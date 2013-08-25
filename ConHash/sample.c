

#include <stdio.h>
#include <stdlib.h>

#include "conhash.h"

struct node_s g_nodes[64];
int main()
{
    int i;
    const struct node_s *node;
    char str[128];
    long hashes[512];

    /* init conhash instance */
    struct conhash_s *conhash = conhash_init(NULL);
    if(conhash)
    {
        /* set nodes */
        conhash_set_node(&g_nodes[0], "node0", 100);
        conhash_set_node(&g_nodes[1], "node1", 100);
        conhash_set_node(&g_nodes[2], "node2", 100);
        conhash_set_node(&g_nodes[3], "node3", 100);
        conhash_set_node(&g_nodes[4], "node4", 100);

        /* add nodes */
        conhash_add_node(conhash, &g_nodes[0]);
        conhash_add_node(conhash, &g_nodes[1]);
        conhash_add_node(conhash, &g_nodes[2]);
        conhash_add_node(conhash, &g_nodes[3]);
        conhash_add_node(conhash, &g_nodes[4]);

        printf("virtual nodes number %d\n", conhash_get_vnodes_num(conhash));
        printf("the hashing results--------------------------------------:\n");

        /* try object */
        for(i = 0; i < 20; i++)
        {
            sprintf(str, "127.0.0.1:10%02d:20%02d", i,i);
            node = conhash_lookup(conhash, str);
            if(node) printf("[%22s] is in node: [%10s]\n", str, node->iden);
        }
        conhash_get_vnodes(conhash, hashes, sizeof(hashes)/sizeof(hashes[0]));
        conhash_del_node(conhash, &g_nodes[2]);
        printf("remove node[%s], virtual nodes number %d\n", g_nodes[2].iden, conhash_get_vnodes_num(conhash));
        printf("the hashing results--------------------------------------:\n");
        for(i = 0; i < 20; i++)
        {
            sprintf(str, "127.0.0.1:10%02d:20%02d", i,i);
            node = conhash_lookup(conhash, str);
            if(node) printf("[%22s] is in node: [%10s]\n", str, node->iden);
        }
    }
    conhash_fini(conhash);
    return 0;
}
