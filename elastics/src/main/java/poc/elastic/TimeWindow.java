package poc.elastic;

import lombok.*;

/**
 * Created by eyallevy on 19/08/17
 */
@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
@ToString
public class TimeWindow {

    Long beforeStartDate;
    Long afterEndDate;
}
