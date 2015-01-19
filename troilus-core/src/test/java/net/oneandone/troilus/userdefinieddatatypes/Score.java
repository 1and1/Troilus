package net.oneandone.troilus.userdefinieddatatypes;


import net.oneandone.troilus.Field;


public class Score {

    @Field(name = "score")
    private Integer score;
        
    
    @SuppressWarnings("unused")
    private Score() {  }
    
    public Score(Integer score) {
        this.score = score;
    }

    public Integer getScore() {
        return score;
    }
}
