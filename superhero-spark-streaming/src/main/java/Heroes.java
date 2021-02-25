import java.io.Serializable;
import java.util.stream.Stream;

public class Heroes implements Serializable{
	
	
	public int id;
    public String name;
    public String gender;
    public String eyecolor;
    public String race;
    public String haircolor;
    public float height;
    public String publisher;
    public String skincolor;
    public String alignment;
    public float weight;


    public Heroes(String []s){

       
    }
    
    
	public static Heroes parseRating(String str ) {
		float height = 0.0f,weight=0.0f;
		  String[] s = str.split(",",-1);
		  if ( s . length != 11) {
			  System.out.println("The elements are ::"); 
			    Stream.of( s ).forEach(System. out ::println);
			    throw new IllegalArgumentException("Each line must contain 10 fields while the current line has ::" + s.length );
		  }
		  
		  
		  Integer id= Integer.parseInt(s[0]);
	        String name = s[1];
	       String gender = s[2];
	        String eyecolor = s[3];
	        String race = s[4];
	        String haircolor = s[5];
	        //to check if its empty string
	        if(!s[6].isEmpty()){
	             height = Float.parseFloat(s[6]);
	        }
	        String publisher = s[7];
	        String skincolor = s[8];
	        String alignment = s[9];
	        //to check if its empty string
	        if(!s[10].isEmpty()){
	             weight = Float.parseFloat(s[10]);
	        }
		  
		  
		
		  return new Heroes(id, name, gender,eyecolor,race,haircolor,height,publisher,skincolor,alignment,weight);
		}



    public Heroes(int id,String name, String gender, String eyecolor, String race, String haircolor, float height, String publisher, String skincolor, String alignment, float weight) {
        this.id = id;
        this.name = name;
        this.gender = gender;
        this.eyecolor = eyecolor;
        this.race = race;
        this.haircolor = haircolor;
        this.height = height;
        this.publisher = publisher;
        this.skincolor = skincolor;
        this.alignment = alignment;
        this.weight = weight;
    }

    @Override
    public String toString() {
        return "Heroes{" +
                "weight='" + weight + '\'' +
                "id='" + id + '\'' +
                "name='" + name + '\'' +
                ", gender='" + gender + '\'' +
                ", eyecolor='" + eyecolor + '\'' +
                ", race='" + race + '\'' +
                ", haircolor='" + haircolor + '\'' +
                ", height='" + height + '\'' +
                ", publisher='" + publisher + '\'' +
                ", skincolor='" + skincolor + '\'' +
                ", alignment='" + alignment + '\'' +

                '}';
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getGender() {
        return gender;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }

    public String getEyecolor() {
        return eyecolor;
    }

    public void setEyecolor(String eyecolor) {
        this.eyecolor = eyecolor;
    }

    public String getRace() {
        return race;
    }

    public void setRace(String race) {
        this.race = race;
    }

    public String getHaircolor() {
        return haircolor;
    }

    public void setHaircolor(String haircolor) {
        this.haircolor = haircolor;
    }

    public float getHeight() {
        return height;
    }

    public void setHeight(float height) {
        this.height = height;
    }

    public String getPublisher() {
        return publisher;
    }

    public void setPublisher(String publisher) {
        this.publisher = publisher;
    }

    public String getSkincolor() {
        return skincolor;
    }

    public void setSkincolor(String skincolor) {
        this.skincolor = skincolor;
    }

    public String getAlignment() {
        return alignment;
    }

    public void setAlignment(String alignment) {
        this.alignment = alignment;
    }

    public float getWeight() {
        return weight;
    }

    public void setWeight(float weight) {
        this.weight = weight;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }


	public Object checkRaceAndWeight(String hero_race, int hero_weight) {
		// TODO Auto-generated method stub
        return race.equals(hero_race) && weight > hero_weight;

	}



}
