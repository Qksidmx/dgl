#include "SkgGraph.h"



int main(int argc, char **argv)
{

    SkgGraph* g = new SkgGraph();
    g->AddEdge("1","2");
    g->AddEdge("1","3");
    g->AddEdge("1","4");
    g->AddEdge("5","1");
    g->AddEdge("6","1");
    std::cout<<"All neighbors of node 1's are: \n";
    g->PrInNbr("1");
    g->PrOutNbr("1");

    delete g;




    /*

    EdgeRequest req;
    req.DisableWAL();
    req.SetEdge(e_label, v_label, "1", v_label, "2");
    s = db->AddEdge(req);
	if (!s.ok()) {
            std::cout << s.ToString() << std::endl;
            return EXIT_FAILURE;
        }
    req.SetEdge(e_label, v_label, "1", v_label, "3");
    s = db->AddEdge(req);
	if (!s.ok()) {
            std::cout << s.ToString() << std::endl;
            return EXIT_FAILURE;
        }
    req.SetEdge(e_label, v_label, "1", v_label, "4");
    s = db->AddEdge(req);
	if (!s.ok()) {
            std::cout << s.ToString() << std::endl;
            return EXIT_FAILURE;
        }
    req.SetEdge(e_label, v_label, "5", v_label, "1");
    s = db->AddEdge(req);
	if (!s.ok()) {
            std::cout << s.ToString() << std::endl;
            return EXIT_FAILURE;
        }
    req.SetEdge(e_label, v_label, "6", v_label, "1");
    s = db->AddEdge(req);
	if (!s.ok()) {
            std::cout << s.ToString() << std::endl;
            return EXIT_FAILURE;
        }
    req.SetEdge(e_label, v_label, "7", v_label, "1");
    s = db->AddEdge(req);
	if (!s.ok()) {
            std::cout << s.ToString() << std::endl;
            return EXIT_FAILURE;
        }
    */

    return 1;
}
