package nearsoft.academy.bigdata.recommendation;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.ThresholdUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.UserBasedRecommender;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import static java.lang.System.exit;

/**
 * Created by jesus on 23/09/16.
 */
public class MovieRecommender {


    int totalReviews, TotalUsers, TotalItems, contando = 0;
    //HashMap para user id
    Map<String, Integer> nameToId = new HashMap<String, Integer>();
    //HashMap para item id
    Map<String, Integer> itemToId = new HashMap<String, Integer>();
    Map<Integer, String> IdToItem = new HashMap<Integer, String>();


    public void process(String filename) throws IOException {

        Map<Integer, List<String>> map = new HashMap<Integer, List<String>>();
        String content, userid = "", itemid = "";
        int lineNumber = 1, j = 0, n = 1, size1 = 1, size2 = 1, sizeitem = 1, while1 = 1;

        //String filename = "/home/jesus/IdeaProjects/movies.txt.gz";
        String encoding = "UTF-8";
        InputStream fileStream = new FileInputStream(filename);
        InputStream gzipStream = new GZIPInputStream(fileStream);
        Reader decoder = new InputStreamReader(gzipStream, encoding);
        BufferedReader decodedBuffer = new BufferedReader(decoder);

        java.io.File file = new java.io.File("dataset.csv");

        // if file doesnt exists, then create it
        if (!file.exists()) {
            file.createNewFile();
        }
        FileWriter fw = new FileWriter(file.getAbsoluteFile());
        BufferedWriter bw = new BufferedWriter(fw);


        while ((content = decodedBuffer.readLine()) != null) {
            String[] line = content.split(":");
            switch (line[0]) {
                case "product/productId":
                    String product = line[1].trim();
                    if (itemToId.size() == 0 || !itemToId.containsKey(product)) {
                        itemToId.put(product, sizeitem);
                        IdToItem.put(sizeitem, product);
                        sizeitem++;
                    }

                    int valueofitem = itemToId.get(product);
                    itemid = Integer.toString(valueofitem);


                    break;
                case "review/userId":
                    String user = line[1].trim();
                    if (nameToId.size() == 0 || !nameToId.containsKey(user)) {
                        nameToId.put(user, size1);
                        size1++;
                    }
                    int valueofuser = nameToId.get(user);
                    userid = Integer.toString(valueofuser);

                    break;

                case "review/score":
                    String score = line[1].trim();


                    String contentfinal = userid + "," + itemid + "," + score + "\n";
                    bw.write(contentfinal);
                    contando++;

                    break;

            }


            lineNumber++;
            n++;

        }//end while

        bw.close();
        fw.close();
        decodedBuffer.close();
        decoder.close();
        gzipStream.close();
        fileStream.close();


        System.out.println("termine de leer el documento");

        totalReviews = contando;//map.size();
        TotalUsers = nameToId.size();
        TotalItems = itemToId.size();
    }


    public int getTotalReviews() {
        return this.totalReviews;
    }

    public int getTotalProducts() {
        return this.TotalItems;
    }

    public int getTotalUsers() {
        return this.TotalUsers;
    }

    public MovieRecommender(String file) throws IOException {
        process(file);
    }


    public List<String> getRecommendationsForUser(String user) throws IOException, TasteException {
        List<String> userRecommendations = new ArrayList<>();
        int userid = this.nameToId.get(user);
        DataModel model = new FileDataModel(new File("dataset.csv"));
        UserSimilarity similarity = new PearsonCorrelationSimilarity(model);
        UserNeighborhood neighborhood = new ThresholdUserNeighborhood(0.1, similarity, model);
        UserBasedRecommender recommender = new GenericUserBasedRecommender(model, neighborhood, similarity);
        List<RecommendedItem> recommendation = recommender.recommend(userid, 3);
        for (RecommendedItem item : recommendation) {
              userRecommendations.add(IdToItem.get((int)item.getItemID()));
        }

        return userRecommendations;
    }
}
